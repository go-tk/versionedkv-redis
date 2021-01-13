package internal

import (
	"time"
)

type EventBusDetails struct {
	Subscriptions             map[string]SubscriptionDetails
	NumberOfIdleSubscriptions int
	IsClosed                  bool
}

type SubscriptionDetails struct {
	NumberOfWatchers int
	IsIdle           bool
	IsExpired        bool
}

func (eb *EventBus) Inspect() EventBusDetails {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eb.IsClosed() {
		return EventBusDetails{
			IsClosed: true,
		}
	}
	var details EventBusDetails
	if eb.subscriptions != nil {
		now := time.Now()
		subscriptionDetails := make(map[string]SubscriptionDetails, len(eb.subscriptions))
		for channelName, subscription := range eb.subscriptions {
			subscriptionIsIdle := !subscription.Deadline.IsZero()
			subscriptionIsExpired := subscriptionIsIdle && !subscription.Deadline.After(now)
			subscriptionDetails[channelName] = SubscriptionDetails{
				NumberOfWatchers: len(subscription.Watchers),
				IsIdle:           subscriptionIsIdle,
				IsExpired:        subscriptionIsExpired,
			}
		}
		details.Subscriptions = subscriptionDetails
	}
	details.NumberOfIdleSubscriptions = eb.idleSubscriptionCount
	return details
}
