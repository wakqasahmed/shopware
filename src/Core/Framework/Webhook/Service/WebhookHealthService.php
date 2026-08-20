<?php declare(strict_types=1);

namespace Shopware\Core\Framework\Webhook\Service;

use Doctrine\DBAL\Connection;
use Psr\Clock\ClockInterface;
use Psr\Log\LoggerInterface;
use Shopware\Core\Defaults;
use Shopware\Core\Framework\DataAbstractionLayer\Doctrine\RetryableTransaction;
use Shopware\Core\Framework\Log\Package;
use Shopware\Core\Framework\Uuid\Uuid;
use Shopware\Core\Framework\Webhook\Event\WebhookActivationTrigger;
use Shopware\Core\Framework\Webhook\Health\DisabledOrigin;
use Shopware\Core\Framework\Webhook\Health\EndpointState;
use Shopware\Core\Framework\Webhook\Health\ErrorClassification;
use Shopware\Core\Framework\Webhook\Health\HealthConfig;
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

        // A success recovers one state at a time.
        if ($state === EndpointState::Suspended && $this->deEscalateSuspendedToDegraded($webhookId)) {
            return;
        }

        if ($state === EndpointState::Degraded && $this->promoteDegradedToHealthy($webhookId, keepFailureStreaks: false)) {
            return;
        }

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
        // Start measuring the interval for which suspension is paused.
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
        return RetryableTransaction::retryable($this->connection, function () use ($webhookId, $trigger): int {
            $id = Uuid::fromHexToBytes($webhookId);

            // Lock the webhook before changing its health or legacy mirror.
            if ($this->connection->fetchOne('SELECT 1 FROM webhook WHERE id = :id FOR UPDATE', ['id' => $id]) === false) {
                return 0;
            }

            $row = $this->connection->fetchAssociative(
                'SELECT endpoint_state, disabled_origin
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

                return 0;
            }

            $fromState = EndpointState::from((string) $row['endpoint_state']);

            $transitioned = $this->reactivationPolicyAllows($trigger, $fromState, $row['disabled_origin'])
                && $this->resetToHealthy($webhookId, keepFailureStreaks: false);

            $this->mirrorBcColumns($webhookId);

            // Refused recoveries repair the mirror but must not release held deliveries.
            if (!$transitioned && $fromState !== EndpointState::Healthy) {
                return 0;
            }

            $this->outboxStore->resumeDeliveriesForWebhook($webhookId);

            return $transitioned ? 1 : 0;
        });
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

    /**
     * Pre-rework `error_count` failure handling. Runs only with WEBHOOKS_REWORK off.
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
     * Pre-rework `error_count` reset. Runs only with WEBHOOKS_REWORK off.
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

        foreach ($candidates as $webhookId) {
            RetryableTransaction::retryable($this->connection, function () use ($webhookId, $now): void {
                // The row lock prevents concurrent ticks from releasing multiple trials.
                $row = $this->lockHealthRow($webhookId);
                if ($row === null) {
                    return;
                }

                $state = EndpointState::from((string) $row['endpoint_state']);
                if ($state !== EndpointState::Degraded && $state !== EndpointState::Suspended) {
                    return;
                }
                if ($row['cooldown_until'] !== null && (string) $row['cooldown_until'] > $now) {
                    return;
                }

                // A trial advances the ladder through its result, not through elapsed time.
                if ($this->outboxStore->hasClaimableOrRunningRows($webhookId)) {
                    return;
                }

                if ($this->outboxStore->releaseOneTrial($webhookId) !== null) {
                    return;
                }

                if ($state === EndpointState::Suspended) {
                    return;
                }

                $this->promoteDegradedToHealthyLocked($webhookId, keepFailureStreaks: true);
            });
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

        /** @var list<string> $candidates */
        $candidates = $this->connection->fetchFirstColumn(
            'SELECT LOWER(HEX(wh.webhook_id))
             FROM webhook_health wh
             LEFT JOIN webhook w ON w.id = wh.webhook_id
             LEFT JOIN app a ON a.id = w.app_id
             WHERE wh.endpoint_state = :suspended
               AND wh.suspended_since IS NOT NULL AND wh.suspended_since <= :cutoff
               AND (a.id IS NULL OR a.active = 1)',
            ['suspended' => EndpointState::Suspended->value, 'cutoff' => $cutoff]
        );

        foreach ($candidates as $webhookId) {
            $disabled = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $cutoff): bool {
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
                    return false;
                }

                return $this->disableRowLocked($webhookId, EndpointState::Suspended, DisabledOrigin::Escalation) > 0;
            });

            if (!$disabled) {
                continue;
            }

            $this->outboxStore->dropBacklogForWebhook($webhookId);
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
            WebhookActivationTrigger::AppReset => !($fromState === EndpointState::Disabled && $disabledOrigin === DisabledOrigin::Operator->value),
            WebhookActivationTrigger::Trial,
            WebhookActivationTrigger::Idle => false,
        };
    }

    /**
     * @param list<EndpointState> $onlyFrom restricts which states may transition
     */
    private function disableFrom(string $webhookId, array $onlyFrom): void
    {
        $disabled = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $onlyFrom): bool {
            $this->ensureHealthRow(Uuid::fromHexToBytes($webhookId), $this->now());
            $row = $this->lockHealthRow($webhookId);
            if ($row === null) {
                return false;
            }

            $fromState = EndpointState::from((string) $row['endpoint_state']);

            if ($fromState === EndpointState::Disabled) {
                return false;
            }

            if (!\in_array($fromState, $onlyFrom, true)) {
                return false;
            }

            return $this->disableRowLocked($webhookId, $fromState, DisabledOrigin::Operator) > 0;
        });

        if (!$disabled) {
            return;
        }

        $this->outboxStore->dropBacklogForWebhook($webhookId);
        $this->logger->warning('Webhook endpoint disabled by operator', ['webhookId' => $webhookId]);
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

        if ($outcome === null) {
            return $this->currentState($webhookId);
        }

        if ($outcome === EndpointState::Degraded) {
            $this->outboxStore->pauseDeliveriesForWebhook($webhookId);
        }

        $this->mirrorBcColumns($webhookId);

        return $outcome;
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
     * Advances a failed trial and returns the resulting state.
     */
    private function advanceLadder(string $webhookId, EndpointState $expected): EndpointState
    {
        $suspended = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $expected): bool {
            $row = $this->lockHealthRow($webhookId);
            if ($row === null || EndpointState::from((string) $row['endpoint_state']) !== $expected) {
                return false;
            }

            return $this->advanceLadderLocked($webhookId, $row, $expected, alsoCountAuthStreak: false);
        });

        if ($suspended) {
            $this->outboxStore->pauseDeliveriesForWebhook($webhookId);
        }

        return $suspended ? EndpointState::Suspended : $expected;
    }

    /**
     * Advances a trial under the row lock; results inside the cooldown are stale.
     *
     * @param array<string, mixed> $row
     */
    private function advanceLadderLocked(string $webhookId, array $row, EndpointState $state, bool $alsoCountAuthStreak): bool
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

            return false;
        }

        $topIndex = \count($this->config->cooldownScheduleSeconds) - 1;
        $nextIndex = (int) $row['degraded_cycle_count'] + 1;

        if ($state === EndpointState::Degraded && $nextIndex > $topIndex) {
            $this->suspendLocked($webhookId, $row, $state, nonTransientFailures: $streak, entryIndex: $topIndex);

            return true;
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

        return false;
    }

    /**
     * Counts auth failures toward suspension; endpoint retirement suspends immediately.
     */
    private function recordNonTransientFailure(string $webhookId, bool $countsStreak): EndpointState
    {
        $result = RetryableTransaction::retryable($this->connection, function () use ($webhookId, $countsStreak): array {
            $this->ensureHealthRow(Uuid::fromHexToBytes($webhookId), $this->now());
            $row = $this->lockHealthRow($webhookId);
            if ($row === null) {
                return [EndpointState::Healthy, false];
            }

            $state = EndpointState::from((string) $row['endpoint_state']);

            if ($state === EndpointState::Suspended) {
                $suspended = $this->advanceLadderLocked($webhookId, $row, $state, alsoCountAuthStreak: $countsStreak);

                return [EndpointState::Suspended, $suspended];
            }

            if ($state === EndpointState::Disabled) {
                return [$state, false];
            }

            $streak = (int) $row['consecutive_non_transient_failures'] + ($countsStreak ? 1 : 0);
            if (!$countsStreak || $streak >= $this->config->nonTransientThreshold) {
                $this->suspendLocked($webhookId, $row, $state, nonTransientFailures: $streak);

                return [EndpointState::Suspended, true];
            }

            if ($state === EndpointState::Degraded) {
                // A below-threshold auth failure still counts as a failed trial.
                $suspended = $this->advanceLadderLocked($webhookId, $row, $state, alsoCountAuthStreak: true);

                return [$suspended ? EndpointState::Suspended : $state, $suspended];
            }

            $this->connection->executeStatement(
                'UPDATE webhook_health
                 SET consecutive_non_transient_failures = :streak, updated_at = :now
                 WHERE webhook_id = :id',
                ['streak' => $streak, 'now' => $this->now(), 'id' => Uuid::fromHexToBytes($webhookId)]
            );
            $this->mirrorBcColumns($webhookId);

            return [$state, false];
        });

        if ($result[1]) {
            $this->outboxStore->pauseDeliveriesForWebhook($webhookId);
        }

        return $result[0];
    }

    /**
     * Suspends a locked row without restarting an existing suspension clock.
     *
     * @param array<string, mixed> $row
     */
    private function suspendLocked(string $webhookId, array $row, EndpointState $fromState, int $nonTransientFailures, int $entryIndex = 0): void
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

        return true;
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

    private function promoteDegradedToHealthy(string $webhookId, bool $keepFailureStreaks): bool
    {
        return RetryableTransaction::retryable($this->connection, function () use ($webhookId, $keepFailureStreaks): bool {
            $row = $this->lockHealthRow($webhookId);
            if ($row === null || (string) $row['endpoint_state'] !== EndpointState::Degraded->value) {
                return false;
            }

            return $this->promoteDegradedToHealthyLocked($webhookId, $keepFailureStreaks);
        });
    }

    private function promoteDegradedToHealthyLocked(string $webhookId, bool $keepFailureStreaks): bool
    {
        if (!$this->resetToHealthy($webhookId, $keepFailureStreaks)) {
            return false;
        }

        // Keep the health row locked until the backlog and BC mirror match the new state.
        $this->outboxStore->resumeDeliveriesForWebhook($webhookId);
        $this->mirrorBcColumns($webhookId);

        return true;
    }

    /**
     * @return array{endpoint_state: string, consecutive_transient_failures: int|string, consecutive_non_transient_failures: int|string, degraded_cycle_count: int|string, cooldown_until: string|null, suspended_since: string|null}|null
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

    private function resetToHealthy(string $webhookId, bool $keepFailureStreaks): bool
    {
        return $this->connection->executeStatement(
            'UPDATE webhook_health
             SET endpoint_state = :healthy,
                 consecutive_transient_failures = IF(:keepFailureStreaks = 1, consecutive_transient_failures, 0),
                 consecutive_non_transient_failures = IF(:keepFailureStreaks = 1, consecutive_non_transient_failures, 0),
                 degraded_cycle_count = 0, cooldown_until = NULL, suspended_since = NULL,
                 disabled_since = NULL, disabled_origin = NULL, updated_at = :now
             WHERE webhook_id = :id AND endpoint_state <> :healthy',
            [
                'healthy' => EndpointState::Healthy->value,
                'keepFailureStreaks' => (int) $keepFailureStreaks,
                'now' => $this->now(),
                'id' => Uuid::fromHexToBytes($webhookId),
            ]
        ) > 0;
    }

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
