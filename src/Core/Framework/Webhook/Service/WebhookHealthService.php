<?php declare(strict_types=1);

namespace Shopware\Core\Framework\Webhook\Service;

use Doctrine\DBAL\Connection;
use Psr\Clock\ClockInterface;
use Psr\EventDispatcher\EventDispatcherInterface;
use Psr\Log\LoggerInterface;
use Shopware\Core\Defaults;
use Shopware\Core\Framework\DataAbstractionLayer\Doctrine\RetryableTransaction;
use Shopware\Core\Framework\Log\Package;
use Shopware\Core\Framework\Uuid\Uuid;
use Shopware\Core\Framework\Webhook\Event\WebhookActivatedEvent;
use Shopware\Core\Framework\Webhook\Event\WebhookActivationTrigger;
use Shopware\Core\Framework\Webhook\Event\WebhookDegradedEvent;
use Shopware\Core\Framework\Webhook\Event\WebhookDisabledEvent;
use Shopware\Core\Framework\Webhook\Event\WebhookSuspendedEvent;
use Shopware\Core\Framework\Webhook\Health\DisabledOrigin;
use Shopware\Core\Framework\Webhook\Health\EndpointState;
use Shopware\Core\Framework\Webhook\Health\ErrorClassification;
use Shopware\Core\Framework\Webhook\Health\HealthConfig;
use Shopware\Core\Framework\Webhook\Health\SuspensionCause;
use Shopware\Core\Framework\Webhook\Health\WebhookDispatchDecision;
use Shopware\Core\Framework\Webhook\Outbox\WebhookOutboxStore;
use Shopware\Core\Framework\Webhook\WebhookException;
use Shopware\Core\Framework\Webhook\WebhookFailureStrategy;
use Shopware\Tests\Integration\Core\Framework\Webhook\Health\EndpointHealthStateMachineMatrixTest;

/**
 * @internal
 *
 * @codeCoverageIgnore
 *
 * @see EndpointHealthStateMachineMatrixTest
 */
#[Package('framework')]
class WebhookHealthService
{
    public function __construct(
        private readonly Connection $connection,
        private readonly WebhookOutboxStore $outboxStore,
        private readonly HealthConfig $config,
        private readonly ClockInterface $clock,
        private readonly EventDispatcherInterface $eventDispatcher,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function gateFor(string $webhookId): WebhookDispatchDecision
    {
        $row = $this->connection->fetchAssociative(
            'SELECT endpoint_state, suspended_since FROM webhook_health WHERE webhook_id = :id',
            ['id' => Uuid::fromHexToBytes($webhookId)]
        );
        if (!\is_array($row)) {
            // Fail-open: a missing health row reads as HEALTHY, so dispatch is never silently blocked.
            return WebhookDispatchDecision::Deliver;
        }

        $state = EndpointState::from((string) $row['endpoint_state']);

        if ($state === EndpointState::Healthy) {
            return WebhookDispatchDecision::Deliver;
        }

        if ($state === EndpointState::Disabled) {
            return WebhookDispatchDecision::Skip;
        }

        if ($state === EndpointState::Degraded && $row['suspended_since'] === null) {
            return WebhookDispatchDecision::Hold;
        }

        // During a suspension incident, only a due trial is delivered; other events are shed.
        return $this->admitIncidentTrial($webhookId);
    }

    public function recordSuccess(string $webhookId): void
    {
        // The guarded writes below absorb a concurrent state change after this read.
        $row = $this->connection->fetchAssociative(
            'SELECT wh.endpoint_state, wh.consecutive_transient_failures, wh.consecutive_non_transient_failures
             FROM webhook_health wh WHERE wh.webhook_id = :id',
            ['id' => Uuid::fromHexToBytes($webhookId)]
        );
        $state = \is_array($row) ? EndpointState::from((string) $row['endpoint_state']) : EndpointState::Healthy;

        // A 2xx moves the webhook up exactly one state. SUSPENDED → DEGRADED: the ladder resets
        // to tier 0 but suspended_since is kept — HEALTHY must be earned through the same ladder.
        // DEGRADED → HEALTHY: full reset, and the held backlog resumes (old events filtered out
        // by age).
        if ($state === EndpointState::Suspended && $this->deEscalateSuspendedToDegraded($webhookId)) {
            return;
        }

        if ($state === EndpointState::Degraded && $this->promoteDegradedToHealthy($webhookId, WebhookActivationTrigger::Trial)) {
            return;
        }

        // HEALTHY with partial streaks: any 2xx resets both failure counters, so failures from
        // separate outages don't add up over time.
        if (\is_array($row) && ((int) $row['consecutive_transient_failures'] > 0 || (int) $row['consecutive_non_transient_failures'] > 0)) {
            $cleared = (int) $this->connection->executeStatement(
                'UPDATE webhook_health
                 SET consecutive_transient_failures = 0, consecutive_non_transient_failures = 0, updated_at = :now
                 WHERE webhook_id = :id AND endpoint_state = :healthy',
                [
                    'now' => $this->now(),
                    'id' => Uuid::fromHexToBytes($webhookId),
                    'healthy' => EndpointState::Healthy->value,
                ]
            );

            if ($cleared > 0) {
                $this->mirrorBcColumns($webhookId);

                return;
            }
        }

        // Health rows are lazy, so also reconcile a fail-open HEALTHY webhook. Success must not
        // reactivate a legacy inactive webhook.
        $this->connection->executeStatement(
            'UPDATE webhook w
             LEFT JOIN webhook_health wh ON wh.webhook_id = w.id
             SET w.error_count = 0
             WHERE w.id = :id
               AND (wh.webhook_id IS NULL OR wh.endpoint_state = :healthy)
               AND w.error_count <> 0',
            [
                'id' => Uuid::fromHexToBytes($webhookId),
                'healthy' => EndpointState::Healthy->value,
            ]
        );
    }

    public function recordFailure(string $webhookId, ErrorClassification $classification, int $attempt): EndpointState
    {
        return match ($classification) {
            ErrorClassification::Success => throw WebhookException::unexpectedClassification($classification->value),
            ErrorClassification::NonTransientPayload => $this->currentState($webhookId),
            ErrorClassification::NonTransientAuth => $this->recordNonTransientFailure($webhookId, countsStreak: true),
            ErrorClassification::NonTransientEndpoint => $this->recordNonTransientFailure($webhookId, countsStreak: false),
            ErrorClassification::TransientNetwork,
            ErrorClassification::TransientServer,
            ErrorClassification::TransientRateLimit,
            ErrorClassification::TransientRedirect => $this->recordTransientFailure($webhookId, $attempt),
        };
    }

    /**
     * Runs scheduled recovery, retirement, and cleanup duties.
     */
    public function tick(): void
    {
        $this->shiftPausedSuspensionClocks();
        $this->runDueReleases();
        $this->retireSuspendedPastBound();
        $this->cancelSurplusSuspendedInFlight();
        $this->healStrandedHolds();
        $this->healPausedOnDisabled();
        $this->healOrphanedHolds();
    }

    public function pauseSuspensionClockForApp(string $appId): void
    {
        // Start measuring the time for which suspension is paused.
        $this->connection->executeStatement(
            'UPDATE webhook_health wh
             JOIN webhook w ON w.id = wh.webhook_id
             SET wh.updated_at = :now
             WHERE w.app_id = :appId AND wh.endpoint_state = :suspended',
            [
                'now' => $this->now(),
                'appId' => Uuid::fromHexToBytes($appId),
                'suspended' => EndpointState::Suspended->value,
            ]
        );
    }

    /**
     * Adds the final paused interval before the app resumes.
     */
    public function resumeSuspensionClockForApp(string $appId): void
    {
        $this->shiftSuspensionClocks('w.app_id = :appId', ['appId' => Uuid::fromHexToBytes($appId)]);
    }

    public function reactivate(string $webhookId, WebhookActivationTrigger $trigger): int
    {
        $event = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $trigger): ?WebhookActivatedEvent {
            $id = Uuid::fromHexToBytes($webhookId);

            // Lock the webhook and read the identity carried by the event.
            $webhookRow = $this->connection->fetchAssociative(
                'SELECT LOWER(HEX(app_id)) AS app_id, name, event_name FROM webhook WHERE id = :id FOR UPDATE',
                ['id' => $id]
            );
            if (!\is_array($webhookRow)) {
                return null;
            }
            $appId = \is_string($webhookRow['app_id']) ? $webhookRow['app_id'] : null;

            $row = $this->connection->fetchAssociative(
                'SELECT endpoint_state, suspended_since, disabled_origin
                 FROM webhook_health WHERE webhook_id = :id FOR UPDATE',
                ['id' => $id]
            );

            if (!\is_array($row)) {
                // A missing health row is HEALTHY, but its legacy mirror may have drifted.
                $this->connection->executeStatement(
                    'UPDATE webhook SET active = 1, error_count = 0 WHERE id = :id',
                    ['id' => $id]
                );
                $this->outboxStore->resumeDeliveriesForWebhook($webhookId);

                return null;
            }

            $fromState = EndpointState::from((string) $row['endpoint_state']);

            $transitioned = $this->reactivationPolicyAllows($trigger, $fromState, $row['disabled_origin'])
                && $this->resetToHealthy($webhookId, keepStreaks: false);

            $this->mirrorBcColumns($webhookId);

            // Refused recoveries repair the BC mirror but must not release held deliveries.
            if (!$transitioned && $fromState !== EndpointState::Healthy) {
                return null;
            }

            $this->outboxStore->resumeDeliveriesForWebhook($webhookId);

            if (!$transitioned) {
                return null;
            }

            return new WebhookActivatedEvent(
                $webhookId,
                $appId,
                $fromState,
                $trigger,
                \is_string($webhookRow['name']) ? $webhookRow['name'] : null,
                \is_string($webhookRow['event_name']) ? $webhookRow['event_name'] : null,
                $this->clock->now(),
                $this->toDateTime($row['suspended_since']),
            );
        });

        if ($event === null) {
            return 0;
        }

        $this->dispatchBestEffort($event);

        return 1;
    }

    public function reactivateForApp(string $appId): void
    {
        // App resets recover every eligible webhook but preserve operator kills.
        /** @var list<string> $webhookIds */
        $webhookIds = $this->connection->fetchFirstColumn(
            'SELECT LOWER(HEX(wh.webhook_id)) FROM webhook_health wh
             JOIN webhook w ON w.id = wh.webhook_id
             WHERE w.app_id = :appId AND wh.endpoint_state <> :healthy',
            [
                'appId' => Uuid::fromHexToBytes($appId),
                'healthy' => EndpointState::Healthy->value,
            ]
        );

        foreach ($webhookIds as $webhookId) {
            $this->reactivate($webhookId, WebhookActivationTrigger::AppReset);
        }
    }

    public function disableByOperatorOnActiveFlip(string $webhookId): void
    {
        // A mirrored active=false write carries intent only when it changes the value.
        $this->disableFrom($webhookId, [EndpointState::Healthy, EndpointState::Degraded]);
    }

    public function disableByOperator(string $webhookId): int
    {
        // The dedicated action carries operator intent in every state.
        return $this->disableFrom($webhookId, null);
    }

    /**
     * @deprecated tag:v6.8.0 - Pre-rework `error_count` failure handling. Runs only with WEBHOOKS_REWORK
     * off and is removed together with the `webhook.active`/`error_count` columns. Renamed from
     * `recordFailure` so the per-delivery {@see recordFailure} can use that name when the
     * flag is on.
     *
     * Increments error_count and applies the strategy. No-op if the webhook is missing or inactive.
     */
    public function recordLegacyFailure(string $webhookId, WebhookFailureStrategy $strategy): void
    {
        $row = $this->connection->fetchAssociative(
            'SELECT active, error_count FROM webhook WHERE id = :id',
            ['id' => Uuid::fromHexToBytes($webhookId)]
        );

        if (!\is_array($row) || !$row['active']) {
            return;
        }

        $newCount = (int) $row['error_count'] + 1;

        $params = $strategy === WebhookFailureStrategy::DisableOnThreshold && $newCount >= WebhookFailureStrategy::MAX_ERROR_COUNT
            ? ['error_count' => 0, 'active' => 0]
            : ['error_count' => $newCount];

        $this->connection->update('webhook', $params, ['id' => Uuid::fromHexToBytes($webhookId)]);
    }

    /**
     * @deprecated tag:v6.8.0 - Pre-rework `error_count` reset. Runs only with WEBHOOKS_REWORK off and is
     * removed together with the legacy columns. With the flag on, {@see recordSuccess} owns the per-webhook reset.
     */
    public function resetErrorCount(string $webhookId): void
    {
        $this->connection->update('webhook', ['error_count' => 0], ['id' => Uuid::fromHexToBytes($webhookId)]);
    }

    /**
     * Releases one due trial or promotes an idle DEGRADED webhook.
     */
    private function runDueReleases(): void
    {
        $now = $this->now();

        /** @var list<string> $candidates */
        $candidates = $this->connection->fetchFirstColumn(
            'SELECT LOWER(HEX(wh.webhook_id))
             FROM webhook_health wh
             LEFT JOIN webhook w ON w.id = wh.webhook_id
             LEFT JOIN app a ON a.id = w.app_id
             WHERE (wh.cooldown_until IS NULL OR wh.cooldown_until <= :now)
               AND (
                    wh.endpoint_state = :degraded
                    OR (wh.endpoint_state = :suspended AND (a.id IS NULL OR a.active = 1))
               )',
            [
                'now' => $now,
                'degraded' => EndpointState::Degraded->value,
                'suspended' => EndpointState::Suspended->value,
            ]
        );

        /** @var list<WebhookActivatedEvent> $idlePromotions */
        $idlePromotions = [];
        foreach ($candidates as $webhookId) {
            // Keep events outside retryable transactions and dispatch them after all candidates.
            $event = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $now): ?WebhookActivatedEvent {
                // The row lock prevents concurrent ticks from releasing multiple trials.
                $row = $this->lockHealthRow($webhookId);
                if ($row === null) {
                    return null;
                }
                $state = EndpointState::from((string) $row['endpoint_state']);
                if ($state !== EndpointState::Degraded && $state !== EndpointState::Suspended) {
                    return null;
                }
                if ($row['cooldown_until'] !== null && (string) $row['cooldown_until'] > $now) {
                    return null;
                }

                // A trial advances the ladder through its result, not through elapsed time.
                if ($this->outboxStore->hasClaimableOrRunningRows($webhookId)) {
                    return null;
                }

                // Releasing a trial does not advance the ladder; its result does.
                if ($this->outboxStore->releaseOneTrial($webhookId) !== null) {
                    return null;
                }

                if ($state === EndpointState::Suspended) {
                    return null;
                }

                return $this->promoteDegradedToHealthyLocked($webhookId, WebhookActivationTrigger::Idle);
            });

            if ($event !== null) {
                $idlePromotions[] = $event;
            }
        }

        foreach ($idlePromotions as $event) {
            $this->dispatchBestEffort($event);
        }
    }

    /**
     * Cancels crash-recovered rows beyond the single SUSPENDED trial.
     */
    private function cancelSurplusSuspendedInFlight(): void
    {
        foreach ($this->outboxStore->findSuspendedWebhookIdsWithClaimableRows() as $webhookId) {
            $this->outboxStore->cancelSurplusInFlightRows($webhookId);
        }
    }

    /**
     * Disables webhooks whose active suspension time exceeds the configured bound.
     */
    private function retireSuspendedPastBound(): void
    {
        $cutoff = $this->clock->now()
            ->modify(\sprintf('-%d days', $this->config->maxSuspendedDays))
            ->format(Defaults::STORAGE_DATE_TIME_FORMAT);

        /** @var list<array{webhook_id: string, app_id: ?string, name: ?string, event_name: ?string}> $candidates */
        $candidates = $this->connection->fetchAllAssociative(
            'SELECT LOWER(HEX(wh.webhook_id)) AS webhook_id, LOWER(HEX(w.app_id)) AS app_id, w.name, w.event_name
             FROM webhook_health wh
             LEFT JOIN webhook w ON w.id = wh.webhook_id
             LEFT JOIN app a ON a.id = w.app_id
             WHERE wh.endpoint_state = :suspended
               AND wh.suspended_since IS NOT NULL AND wh.suspended_since <= :cutoff
               AND (a.id IS NULL OR a.active = 1)',
            ['suspended' => EndpointState::Suspended->value, 'cutoff' => $cutoff]
        );

        foreach ($candidates as $candidate) {
            $webhookId = $candidate['webhook_id'];
            $event = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $candidate, $cutoff): ?WebhookDisabledEvent {
                // A busy or recovered candidate is left for a later tick.
                $locked = $this->connection->fetchOne(
                    'SELECT 1 FROM webhook_health
                     WHERE webhook_id = :id AND endpoint_state = :suspended AND suspended_since <= :cutoff
                     FOR UPDATE SKIP LOCKED',
                    [
                        'id' => Uuid::fromHexToBytes($webhookId),
                        'suspended' => EndpointState::Suspended->value,
                        'cutoff' => $cutoff,
                    ]
                );
                if ($locked === false) {
                    return null;
                }

                if ($this->disableRowLocked($webhookId, EndpointState::Suspended, DisabledOrigin::Escalation) === 0) {
                    return null;
                }

                return new WebhookDisabledEvent(
                    $webhookId,
                    $candidate['app_id'],
                    EndpointState::Suspended,
                    DisabledOrigin::Escalation,
                    $candidate['name'],
                    $candidate['event_name'],
                    $this->clock->now(),
                );
            });

            if ($event === null) {
                continue;
            }

            $this->outboxStore->dropBacklogForWebhook($webhookId);
            $this->dispatchBestEffort($event);
            $this->logger->warning('Webhook endpoint disabled after exceeding the suspension bound', [
                'webhookId' => $webhookId,
                'maxSuspendedDays' => $this->config->maxSuspendedDays,
            ]);
        }
    }

    /**
     * Resumes rows held by a gate/recovery race on a HEALTHY webhook.
     */
    private function healStrandedHolds(): void
    {
        foreach ($this->outboxStore->findWebhookIdsWithStrandedHolds() as $webhookId) {
            $this->outboxStore->resumeDeliveriesForWebhook($webhookId);
        }
    }

    /**
     * Drops rows held by a gate/disable race on a DISABLED webhook.
     */
    private function healPausedOnDisabled(): void
    {
        foreach ($this->outboxStore->findDisabledWebhookIdsWithHeldRows() as $webhookId) {
            $this->outboxStore->dropBacklogForWebhook($webhookId);
        }
    }

    /**
     * Cancels held rows whose webhook was deleted.
     */
    private function healOrphanedHolds(): void
    {
        $this->outboxStore->cancelOrphanedHeldRows();
    }

    /**
     * Shifts suspension time by the interval since the last inactive-app tick.
     */
    private function shiftPausedSuspensionClocks(): void
    {
        $this->shiftSuspensionClocks('a.active = 0');
    }

    /**
     * Moves suspension clocks forward by the interval since each row was last touched, so time an
     * app spends deactivated never counts toward the retirement bound.
     *
     * @param array<string, mixed> $params parameters referenced by $scope
     */
    private function shiftSuspensionClocks(string $scope, array $params = []): void
    {
        $this->connection->executeStatement(
            \sprintf(
                'UPDATE webhook_health wh
                 JOIN webhook w ON w.id = wh.webhook_id
                 LEFT JOIN app a ON a.id = w.app_id
                 JOIN (SELECT webhook_id, updated_at AS cursor_at FROM webhook_health
                       WHERE endpoint_state = :suspended AND updated_at IS NOT NULL) snap
                   ON snap.webhook_id = wh.webhook_id
                 SET wh.suspended_since = TIMESTAMPADD(MICROSECOND, TIMESTAMPDIFF(MICROSECOND, snap.cursor_at, :now), wh.suspended_since),
                     wh.updated_at = :now
                 WHERE %s
                   AND wh.endpoint_state = :suspended
                   AND wh.suspended_since IS NOT NULL
                   AND snap.cursor_at < :now',
                $scope
            ),
            [
                ...$params,
                'now' => $this->now(),
                'suspended' => EndpointState::Suspended->value,
            ]
        );
    }

    /**
     * Keeps operator kills out of automated recovery paths.
     */
    private function reactivationPolicyAllows(WebhookActivationTrigger $trigger, EndpointState $fromState, mixed $disabledOrigin): bool
    {
        if ($fromState === EndpointState::Healthy) {
            return false;
        }

        return match ($trigger) {
            WebhookActivationTrigger::Manual => $fromState === EndpointState::Suspended || $fromState === EndpointState::Disabled,
            WebhookActivationTrigger::AppReset,
            WebhookActivationTrigger::AppReactivateApi => !($fromState === EndpointState::Disabled && $disabledOrigin === DisabledOrigin::Operator->value),
            WebhookActivationTrigger::Trial,
            WebhookActivationTrigger::Idle => false,
        };
    }

    /**
     * @param list<EndpointState>|null $onlyFrom restricts which states may transition; null = any
     */
    private function disableFrom(string $webhookId, ?array $onlyFrom): int
    {
        $event = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $onlyFrom): ?WebhookDisabledEvent {
            $this->ensureHealthRow(Uuid::fromHexToBytes($webhookId), $this->now());
            $row = $this->lockHealthRow($webhookId);
            if ($row === null) {
                return null;
            }

            $fromState = EndpointState::from((string) $row['endpoint_state']);

            if ($fromState === EndpointState::Disabled) {
                // The dedicated action turns an escalation disable into an operator kill.
                if ($onlyFrom === null) {
                    $this->connection->executeStatement(
                        'UPDATE webhook_health SET disabled_origin = :origin, updated_at = :now
                         WHERE webhook_id = :id AND endpoint_state = :disabled',
                        [
                            'origin' => DisabledOrigin::Operator->value,
                            'now' => $this->now(),
                            'id' => Uuid::fromHexToBytes($webhookId),
                            'disabled' => EndpointState::Disabled->value,
                        ]
                    );
                }

                return null;
            }

            if ($onlyFrom !== null && !\in_array($fromState, $onlyFrom, true)) {
                return null;
            }

            if ($this->disableRowLocked($webhookId, $fromState, DisabledOrigin::Operator) === 0) {
                return null;
            }

            $ref = $this->webhookRefOf($webhookId);

            return new WebhookDisabledEvent(
                $webhookId,
                $ref['appId'],
                $fromState,
                DisabledOrigin::Operator,
                $ref['name'],
                $ref['eventName'],
                $this->clock->now(),
            );
        });

        if ($event === null) {
            return 0;
        }

        $this->outboxStore->dropBacklogForWebhook($webhookId);
        $this->dispatchBestEffort($event);
        $this->logger->warning('Webhook endpoint disabled by operator', ['webhookId' => $webhookId]);

        return 1;
    }

    /**
     * Transitions a row already locked by the caller.
     */
    private function disableRowLocked(string $webhookId, EndpointState $fromState, DisabledOrigin $origin): int
    {
        $disabled = (int) $this->connection->executeStatement(
            'UPDATE webhook_health
             SET endpoint_state = :disabled, disabled_since = :now, disabled_origin = :origin,
                 cooldown_until = NULL, updated_at = :now
             WHERE webhook_id = :id AND endpoint_state = :from',
            [
                'disabled' => EndpointState::Disabled->value,
                'now' => $this->now(),
                'origin' => $origin->value,
                'id' => Uuid::fromHexToBytes($webhookId),
                'from' => $fromState->value,
            ]
        );

        if ($disabled > 0) {
            $this->mirrorBcColumns($webhookId);
        }

        return $disabled;
    }

    private function recordTransientFailure(string $webhookId, int $attempt): EndpointState
    {
        $current = $this->currentState($webhookId);

        if ($current === EndpointState::Degraded || $current === EndpointState::Suspended) {
            return $this->advanceLadder($webhookId, $current);
        }

        if ($current === EndpointState::Disabled) {
            return $current;
        }

        // Retries of the same delivery do not count towards endpoint health.
        if ($attempt > 1) {
            return $current;
        }

        return $this->recordHealthyTransientFailure($webhookId);
    }

    private function recordHealthyTransientFailure(string $webhookId): EndpointState
    {
        $threshold = $this->config->degradedThreshold;
        $now = $this->now();
        $firstCooldown = $this->cooldownAt(0);
        $webhookIdBytes = Uuid::fromHexToBytes($webhookId);

        $outcome = RetryableTransaction::retryable($this->connection, function () use ($webhookIdBytes, $threshold, $now, $firstCooldown): ?EndpointState {
            $this->ensureHealthRow($webhookIdBytes, $now);

            return $this->updateHealthyTransientFailure($webhookIdBytes, $threshold, $now, $firstCooldown);
        });

        if ($outcome === EndpointState::Degraded) {
            // Hold the rest of the backlog for the ladder; the result side holds the in-flight row itself.
            $this->outboxStore->pauseDeliveriesForWebhook($webhookId);
            $ref = $this->webhookRefOf($webhookId);
            $this->dispatchBestEffort(new WebhookDegradedEvent(
                $webhookId,
                $ref['appId'],
                EndpointState::Healthy,
                $ref['name'],
                $ref['eventName'],
                $this->clock->now(),
            ));
        }

        if ($outcome !== null) {
            $this->mirrorBcColumns($webhookId);

            return $outcome;
        }

        // Another writer moved the state — report what it is now.
        return $this->currentState($webhookId);
    }

    private function updateHealthyTransientFailure(
        string $webhookIdBytes,
        int $threshold,
        string $now,
        string $firstCooldown,
    ): ?EndpointState {
        $incremented = (int) $this->connection->executeStatement(
            'UPDATE webhook_health
             SET consecutive_transient_failures = consecutive_transient_failures + 1, updated_at = :now
             WHERE webhook_id = :id AND endpoint_state = :healthy
               AND consecutive_transient_failures + 1 < :threshold',
            [
                'healthy' => EndpointState::Healthy->value,
                'now' => $now,
                'id' => $webhookIdBytes,
                'threshold' => $threshold,
            ]
        );
        if ($incremented > 0) {
            return EndpointState::Healthy;
        }

        $crossed = (int) $this->connection->executeStatement(
            'UPDATE webhook_health
             SET endpoint_state = :degraded, degraded_cycle_count = 0, cooldown_until = :firstCooldown,
                 consecutive_transient_failures = consecutive_transient_failures + 1, updated_at = :now
             WHERE webhook_id = :id AND endpoint_state = :healthy
               AND consecutive_transient_failures + 1 >= :threshold',
            [
                'degraded' => EndpointState::Degraded->value,
                'healthy' => EndpointState::Healthy->value,
                'firstCooldown' => $firstCooldown,
                'now' => $now,
                'id' => $webhookIdBytes,
                'threshold' => $threshold,
            ]
        );

        return $crossed > 0 ? EndpointState::Degraded : null;
    }

    /**
     * Health rows are created on first use; the conflict clause absorbs a concurrent writer.
     */
    private function ensureHealthRow(string $webhookIdBytes, string $now): void
    {
        $this->connection->executeStatement(
            'INSERT INTO webhook_health (webhook_id, endpoint_state, created_at)
             VALUES (:id, :healthy, :now)
             ON DUPLICATE KEY UPDATE webhook_id = webhook_id',
            [
                'id' => $webhookIdBytes,
                'healthy' => EndpointState::Healthy->value,
                'now' => $now,
            ]
        );
    }

    /**
     * Counts auth failures toward suspension; endpoint retirement suspends immediately.
     */
    private function recordNonTransientFailure(string $webhookId, bool $countsStreak): EndpointState
    {
        $cause = $countsStreak ? SuspensionCause::AuthStreak : SuspensionCause::Gone;
        $result = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $countsStreak, $cause): array {
            $this->ensureHealthRow(Uuid::fromHexToBytes($webhookId), $this->now());
            $row = $this->lockHealthRow($webhookId);
            if ($row === null) {
                return [EndpointState::Healthy, null];
            }

            $state = EndpointState::from((string) $row['endpoint_state']);

            if ($state === EndpointState::Suspended) {
                $event = $this->advanceLadderLocked($webhookId, $row, $state, alsoCountAuthStreak: $countsStreak);

                return [EndpointState::Suspended, $event];
            }

            if ($state === EndpointState::Disabled) {
                return [$state, null];
            }

            $streak = (int) $row['consecutive_non_transient_failures'] + ($countsStreak ? 1 : 0);
            if (!$countsStreak || $streak >= $this->config->nonTransientThreshold) {
                return [EndpointState::Suspended, $this->suspendLocked($webhookId, $row, $state, nonTransientFailures: $streak, cause: $cause)];
            }

            if ($state === EndpointState::Degraded) {
                // A below-threshold auth failure still counts as a failed trial.
                $event = $this->advanceLadderLocked($webhookId, $row, $state, alsoCountAuthStreak: true);

                return [$event !== null ? EndpointState::Suspended : $state, $event];
            }

            $this->connection->executeStatement(
                'UPDATE webhook_health
                 SET consecutive_non_transient_failures = :streak, updated_at = :now
                 WHERE webhook_id = :id',
                ['streak' => $streak, 'now' => $this->now(), 'id' => Uuid::fromHexToBytes($webhookId)]
            );
            $this->mirrorBcColumns($webhookId);

            return [$state, null];
        });

        $this->finishSuspension($webhookId, $result[1]);

        return $result[0];
    }

    /**
     * Advances a failed trial and returns the resulting state.
     */
    private function advanceLadder(string $webhookId, EndpointState $expected): EndpointState
    {
        $suspension = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $expected): ?WebhookSuspendedEvent {
            $row = $this->lockHealthRow($webhookId);
            if ($row === null || EndpointState::from((string) $row['endpoint_state']) !== $expected) {
                // Ignore a result for a state that changed concurrently.
                return null;
            }

            return $this->advanceLadderLocked($webhookId, $row, $expected, alsoCountAuthStreak: false);
        });

        $this->finishSuspension($webhookId, $suspension);

        return $suspension !== null ? EndpointState::Suspended : $expected;
    }

    /**
     * Advances a trial under the row lock; results inside the cooldown are stale.
     *
     * @param array<string, mixed> $row
     */
    private function advanceLadderLocked(string $webhookId, array $row, EndpointState $state, bool $alsoCountAuthStreak): ?WebhookSuspendedEvent
    {
        $now = $this->now();
        $id = Uuid::fromHexToBytes($webhookId);
        $cooldownElapsed = $row['cooldown_until'] === null || (string) $row['cooldown_until'] <= $now;
        $streak = (int) $row['consecutive_non_transient_failures'] + ($alsoCountAuthStreak ? 1 : 0);

        if (!$cooldownElapsed) {
            if ($alsoCountAuthStreak) {
                // The auth streak is independent of the trial ladder.
                $this->connection->executeStatement(
                    'UPDATE webhook_health SET consecutive_non_transient_failures = :streak, updated_at = :now WHERE webhook_id = :id',
                    ['streak' => $streak, 'now' => $now, 'id' => $id]
                );
                $this->mirrorBcColumns($webhookId);
            }

            return null;
        }

        $topIndex = \count($this->config->cooldownScheduleSeconds) - 1;
        $nextIndex = (int) $row['degraded_cycle_count'] + 1;

        if ($state === EndpointState::Degraded && $nextIndex > $topIndex) {
            return $this->suspendLocked($webhookId, $row, $state, nonTransientFailures: $streak, cause: SuspensionCause::ScheduleExhausted, entryIndex: $topIndex);
        }

        $index = min($nextIndex, $topIndex);
        $this->connection->executeStatement(
            'UPDATE webhook_health
             SET degraded_cycle_count = :index, cooldown_until = :cooldown,
                 consecutive_non_transient_failures = :streak, updated_at = :now
             WHERE webhook_id = :id',
            [
                'index' => $index,
                'cooldown' => $this->cooldownAt($index),
                'streak' => $streak,
                'now' => $now,
                'id' => $id,
            ]
        );

        if ($alsoCountAuthStreak) {
            $this->mirrorBcColumns($webhookId);
        }

        return null;
    }

    /**
     * Suspends a locked row without restarting an existing suspension clock.
     *
     * @param array<string, mixed> $row
     */
    private function suspendLocked(string $webhookId, array $row, EndpointState $fromState, int $nonTransientFailures, SuspensionCause $cause, int $entryIndex = 0): WebhookSuspendedEvent
    {
        $now = $this->now();
        $since = $row['suspended_since'] !== null ? (string) $row['suspended_since'] : $now;

        $this->connection->executeStatement(
            'UPDATE webhook_health
             SET endpoint_state = :suspended, suspended_since = :since, degraded_cycle_count = :index,
                 cooldown_until = :cooldown, consecutive_non_transient_failures = :streak, updated_at = :now
             WHERE webhook_id = :id AND endpoint_state = :from',
            [
                'suspended' => EndpointState::Suspended->value,
                'since' => $since,
                'index' => $entryIndex,
                'cooldown' => $this->cooldownAt($entryIndex),
                'streak' => $nonTransientFailures,
                'now' => $now,
                'id' => Uuid::fromHexToBytes($webhookId),
                'from' => $fromState->value,
            ]
        );
        $this->mirrorBcColumns($webhookId);

        $ref = $this->webhookRefOf($webhookId);

        return new WebhookSuspendedEvent(
            $webhookId,
            $ref['appId'],
            $fromState,
            new \DateTimeImmutable($since),
            $cause,
            $ref['name'],
            $ref['eventName'],
            new \DateTimeImmutable($now),
        );
    }

    private function finishSuspension(string $webhookId, ?WebhookSuspendedEvent $suspension): void
    {
        if ($suspension === null) {
            return;
        }

        $this->outboxStore->pauseDeliveriesForWebhook($webhookId);
        $this->dispatchBestEffort($suspension);
    }

    /**
     * Moves a successful SUSPENDED trial to DEGRADED while preserving the incident clock.
     */
    private function deEscalateSuspendedToDegraded(string $webhookId): bool
    {
        $deEscalated = (int) $this->connection->executeStatement(
            'UPDATE webhook_health
             SET endpoint_state = :degraded, degraded_cycle_count = 0, cooldown_until = :cooldown,
                 consecutive_transient_failures = 0, consecutive_non_transient_failures = 0, updated_at = :now
             WHERE webhook_id = :id AND endpoint_state = :suspended',
            [
                'degraded' => EndpointState::Degraded->value,
                'cooldown' => $this->cooldownAt(0),
                'now' => $this->now(),
                'id' => Uuid::fromHexToBytes($webhookId),
                'suspended' => EndpointState::Suspended->value,
            ]
        );

        if ($deEscalated === 0) {
            return false;
        }

        $this->mirrorBcColumns($webhookId);
        $ref = $this->webhookRefOf($webhookId);
        $this->dispatchBestEffort(new WebhookDegradedEvent(
            $webhookId,
            $ref['appId'],
            EndpointState::Suspended,
            $ref['name'],
            $ref['eventName'],
            $this->clock->now(),
        ));

        return true;
    }

    /**
     * Promotes DEGRADED to HEALTHY and dispatches after commit.
     */
    private function promoteDegradedToHealthy(string $webhookId, WebhookActivationTrigger $trigger): bool
    {
        $event = RetryableTransaction::retryable(
            $this->connection,
            fn (): ?WebhookActivatedEvent => $this->promoteDegradedToHealthyLocked($webhookId, $trigger)
        );

        if ($event === null) {
            return false;
        }

        $this->dispatchBestEffort($event);

        return true;
    }

    /**
     * Promotes inside the caller's transaction without dispatching the event.
     */
    private function promoteDegradedToHealthyLocked(string $webhookId, WebhookActivationTrigger $trigger): ?WebhookActivatedEvent
    {
        $row = $this->lockHealthRow($webhookId);
        if ($row === null || (string) $row['endpoint_state'] !== EndpointState::Degraded->value) {
            return null;
        }

        if (!$this->resetToHealthy($webhookId, keepStreaks: $trigger === WebhookActivationTrigger::Idle)) {
            return null;
        }

        $this->outboxStore->resumeDeliveriesForWebhook($webhookId);
        $this->mirrorBcColumns($webhookId);

        $ref = $this->webhookRefOf($webhookId);

        return new WebhookActivatedEvent(
            $webhookId,
            $ref['appId'],
            EndpointState::Degraded,
            $trigger,
            $ref['name'],
            $ref['eventName'],
            $this->clock->now(),
            $this->toDateTime($row['suspended_since']),
        );
    }

    /**
     * @return array{endpoint_state: string, consecutive_transient_failures: int|string, consecutive_non_transient_failures: int|string, degraded_cycle_count: int|string, cooldown_until: string|null, suspended_since: string|null}|null the FOR-UPDATE-locked webhook_health row, or null when none exists
     */
    private function lockHealthRow(string $webhookId): ?array
    {
        /** @var array{endpoint_state: string, consecutive_transient_failures: int|string, consecutive_non_transient_failures: int|string, degraded_cycle_count: int|string, cooldown_until: string|null, suspended_since: string|null}|false $row */
        $row = $this->connection->fetchAssociative(
            'SELECT endpoint_state, consecutive_transient_failures, consecutive_non_transient_failures,
                    degraded_cycle_count, cooldown_until, suspended_since
             FROM webhook_health WHERE webhook_id = :id FOR UPDATE',
            ['id' => Uuid::fromHexToBytes($webhookId)]
        );

        return $row === false ? null : $row;
    }

    /**
     * Admits one natural-traffic trial and re-arms the cooldown atomically.
     */
    private function admitIncidentTrial(string $webhookId): WebhookDispatchDecision
    {
        // Avoid locking when a scheduled trial is already available.
        if ($this->outboxStore->hasHeldRows($webhookId)) {
            return WebhookDispatchDecision::Skip;
        }

        $admitted = RetryableTransaction::retryable($this->connection, function () use ($webhookId): bool {
            $row = $this->lockHealthRow($webhookId);
            if ($row === null) {
                return false;
            }
            // Re-check the unlocked gate decision under the row lock.
            $state = EndpointState::from((string) $row['endpoint_state']);
            $inIncident = $state === EndpointState::Suspended
                || ($state === EndpointState::Degraded && $row['suspended_since'] !== null);
            if (!$inIncident) {
                return false;
            }
            if ($row['cooldown_until'] !== null && (string) $row['cooldown_until'] > $this->now()) {
                return false;
            }
            if ($this->outboxStore->hasHeldRows($webhookId) || $this->outboxStore->hasClaimableOrRunningRows($webhookId)) {
                return false;
            }

            $top = \count($this->config->cooldownScheduleSeconds) - 1;
            $index = min((int) $row['degraded_cycle_count'] + 1, $top);
            $this->connection->executeStatement(
                'UPDATE webhook_health
                 SET degraded_cycle_count = :index, cooldown_until = :cooldown, updated_at = :now
                 WHERE webhook_id = :id AND endpoint_state = :state',
                [
                    'index' => $index,
                    'cooldown' => $this->cooldownAt($index),
                    'now' => $this->now(),
                    'id' => Uuid::fromHexToBytes($webhookId),
                    'state' => $state->value,
                ]
            );

            return true;
        });

        return $admitted ? WebhookDispatchDecision::Deliver : WebhookDispatchDecision::Skip;
    }

    /**
     * Resets the episode; idle promotion keeps unproven failure streaks.
     */
    private function resetToHealthy(string $webhookId, bool $keepStreaks): bool
    {
        return $this->connection->executeStatement(
            'UPDATE webhook_health
             SET endpoint_state = :healthy,
                 consecutive_transient_failures = IF(:keepStreaks = 1, consecutive_transient_failures, 0),
                 consecutive_non_transient_failures = IF(:keepStreaks = 1, consecutive_non_transient_failures, 0),
                 degraded_cycle_count = 0, cooldown_until = NULL, suspended_since = NULL,
                 disabled_since = NULL, disabled_origin = NULL, updated_at = :now
             WHERE webhook_id = :id AND endpoint_state <> :healthy',
            [
                'healthy' => EndpointState::Healthy->value,
                'keepStreaks' => (int) $keepStreaks,
                'now' => $this->now(),
                'id' => Uuid::fromHexToBytes($webhookId),
            ]
        ) > 0;
    }

    /**
     * Derives the legacy BC columns from the current health row to avoid stale writes.
     */
    private function mirrorBcColumns(string $webhookId): void
    {
        $this->connection->executeStatement(
            'UPDATE webhook w
             JOIN webhook_health wh ON wh.webhook_id = w.id
             SET w.active = IF(wh.endpoint_state IN (:healthy, :degraded), 1, 0),
                 w.error_count = IF(
                     wh.endpoint_state = :healthy,
                     0,
                     GREATEST(wh.consecutive_transient_failures, wh.consecutive_non_transient_failures)
                 )
             WHERE w.id = :id',
            [
                'healthy' => EndpointState::Healthy->value,
                'degraded' => EndpointState::Degraded->value,
                'id' => Uuid::fromHexToBytes($webhookId),
            ]
        );
    }

    /**
     * Lifecycle events are advisory and must not affect the committed transition.
     */
    private function dispatchBestEffort(object $event): void
    {
        try {
            $this->eventDispatcher->dispatch($event);
        } catch (\Throwable $e) {
            $this->logger->warning('Webhook lifecycle event listener failed', [
                'event' => $event::class,
                'exception' => $e::class,
            ]);
        }
    }

    /**
     * @return array{appId: ?string, name: ?string, eventName: ?string}
     */
    private function webhookRefOf(string $webhookId): array
    {
        $row = $this->connection->fetchAssociative(
            'SELECT LOWER(HEX(app_id)) AS app_id, name, event_name FROM webhook WHERE id = :id',
            ['id' => Uuid::fromHexToBytes($webhookId)]
        );

        return [
            'appId' => \is_array($row) && \is_string($row['app_id']) ? $row['app_id'] : null,
            'name' => \is_array($row) && \is_string($row['name']) ? $row['name'] : null,
            'eventName' => \is_array($row) && \is_string($row['event_name']) ? $row['event_name'] : null,
        ];
    }

    private function toDateTime(mixed $storageValue): ?\DateTimeImmutable
    {
        return \is_string($storageValue) ? new \DateTimeImmutable($storageValue) : null;
    }

    private function cooldownAt(int $index): string
    {
        $schedule = $this->config->cooldownScheduleSeconds;
        $seconds = $schedule[min($index, \count($schedule) - 1)];

        return $this->clock->now()
            ->modify(\sprintf('+%d seconds', $seconds))
            ->format(Defaults::STORAGE_DATE_TIME_FORMAT);
    }

    private function currentState(string $webhookId): EndpointState
    {
        $state = $this->connection->fetchOne(
            'SELECT endpoint_state FROM webhook_health WHERE webhook_id = :id',
            ['id' => Uuid::fromHexToBytes($webhookId)]
        );

        // Missing health rows fail open during rollout.
        return $state === false ? EndpointState::Healthy : EndpointState::from((string) $state);
    }

    private function now(): string
    {
        return $this->clock->now()->format(Defaults::STORAGE_DATE_TIME_FORMAT);
    }
}
