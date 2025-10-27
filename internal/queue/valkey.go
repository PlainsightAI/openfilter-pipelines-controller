/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"context"
	"fmt"
	"strconv"

	"github.com/valkey-io/valkey-go"
)

// ValkeyClient wraps Valkey stream operations for pipeline runs
type ValkeyClient struct {
	client valkey.Client
}

// NewValkeyClient creates a new Valkey client from connection options
func NewValkeyClient(addr, password string) (*ValkeyClient, error) {
	clientOpts := valkey.ClientOption{
		InitAddress: []string{addr},
	}

	if password != "" {
		clientOpts.Password = password
	}

	client, err := valkey.NewClient(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create valkey client: %w", err)
	}

	return &ValkeyClient{
		client: client,
	}, nil
}

// Close closes the Valkey client connection
func (v *ValkeyClient) Close() {
	v.client.Close()
}

// CreateStreamAndGroup creates a Valkey stream and consumer group for a pipeline run
// If the stream doesn't exist, it creates it with MKSTREAM
// Then creates the consumer group starting from the beginning of the stream
func (v *ValkeyClient) CreateStreamAndGroup(ctx context.Context, streamKey, groupName string) error {
	// Try to create the consumer group with MKSTREAM
	cmd := v.client.B().XgroupCreate().
		Key(streamKey).
		Group(groupName).
		Id("0").
		Mkstream().
		Build()

	err := v.client.Do(ctx, cmd).Error()
	if err != nil {
		// Check if group already exists (BUSYGROUP error)
		errMsg := err.Error()
		if errMsg == "BUSYGROUP Consumer Group name already exists" {
			return nil // Group already exists, which is fine
		}
		return fmt.Errorf("failed to create stream and group: %w", err)
	}

	return nil
}

// EnqueueFileWithAttempts adds a file to the work stream with a specific attempts count
// Message format: {run: runId, file: filepath, attempts: attemptsCount}
func (v *ValkeyClient) EnqueueFileWithAttempts(ctx context.Context, streamKey, runID, filepath string, attempts int) (string, error) {
	cmd := v.client.B().Xadd().
		Key(streamKey).
		Id("*").
		FieldValue().
		FieldValue("run", runID).
		FieldValue("file", filepath).
		FieldValue("attempts", strconv.Itoa(attempts)).
		Build()

	result := v.client.Do(ctx, cmd)
	if result.Error() != nil {
		return "", fmt.Errorf("failed to enqueue file %s: %w", filepath, result.Error())
	}

	messageID, err := result.ToString()
	if err != nil {
		return "", fmt.Errorf("failed to get message ID: %w", err)
	}

	return messageID, nil
}

// GetStreamLength returns the number of messages in a stream
func (v *ValkeyClient) GetStreamLength(ctx context.Context, streamKey string) (int64, error) {
	cmd := v.client.B().Xlen().Key(streamKey).Build()
	result := v.client.Do(ctx, cmd)

	if result.Error() != nil {
		return 0, fmt.Errorf("failed to get stream length: %w", result.Error())
	}

	length, err := result.AsInt64()
	if err != nil {
		return 0, fmt.Errorf("failed to parse stream length: %w", err)
	}

	return length, nil
}

// GetPendingCount returns the number of pending messages in a consumer group
func (v *ValkeyClient) GetPendingCount(ctx context.Context, streamKey, groupName string) (int64, error) {
	cmd := v.client.B().Xpending().
		Key(streamKey).
		Group(groupName).
		Build()

	result := v.client.Do(ctx, cmd)
	if result.Error() != nil {
		return 0, fmt.Errorf("failed to get pending count: %w", result.Error())
	}

	// XPENDING returns an array where the first element is the count
	arr, err := result.ToArray()
	if err != nil {
		return 0, fmt.Errorf("failed to parse pending result: %w", err)
	}

	if len(arr) == 0 {
		return 0, nil
	}

	count, err := arr[0].AsInt64()
	if err != nil {
		return 0, fmt.Errorf("failed to parse pending count: %w", err)
	}

	return count, nil
}

// AckMessage acknowledges a message in the stream
func (v *ValkeyClient) AckMessage(ctx context.Context, streamKey, groupName, messageID string) error {
	cmd := v.client.B().Xack().
		Key(streamKey).
		Group(groupName).
		Id(messageID).
		Build()

	err := v.client.Do(ctx, cmd).Error()
	if err != nil {
		return fmt.Errorf("failed to ack message %s: %w", messageID, err)
	}
	return nil
}

// XMessage represents a message from the stream
type XMessage struct {
	ID     string
	Values map[string]string
}

// AutoClaim reclaims pending messages that have been idle for too long
// Returns the reclaimed messages
func (v *ValkeyClient) AutoClaim(ctx context.Context, streamKey, groupName, consumerName string, minIdleTime int64, count int64) ([]XMessage, error) {
	cmd := v.client.B().Xautoclaim().
		Key(streamKey).
		Group(groupName).
		Consumer(consumerName).
		MinIdleTime(strconv.FormatInt(minIdleTime, 10)).
		Start("0-0").
		Count(count).
		Build()

	result := v.client.Do(ctx, cmd)
	if result.Error() != nil {
		return nil, fmt.Errorf("failed to auto-claim messages: %w", result.Error())
	}

	// XAUTOCLAIM returns [start-id, [messages], [deleted-ids]]
	arr, err := result.ToArray()
	if err != nil {
		return nil, fmt.Errorf("failed to parse autoclaim result: %w", err)
	}

	if len(arr) < 2 {
		return []XMessage{}, nil
	}

	// Parse messages array
	messagesArr, err := arr[1].ToArray()
	if err != nil {
		return nil, fmt.Errorf("failed to parse messages array: %w", err)
	}

	messages := make([]XMessage, 0, len(messagesArr))
	for _, msgVal := range messagesArr {
		msgArr, err := msgVal.ToArray()
		if err != nil {
			continue
		}

		if len(msgArr) < 2 {
			continue
		}

		msgID, _ := msgArr[0].ToString()
		fieldsArr, err := msgArr[1].ToArray()
		if err != nil {
			continue
		}

		values := make(map[string]string)
		for i := 0; i < len(fieldsArr)-1; i += 2 {
			key, _ := fieldsArr[i].ToString()
			val, _ := fieldsArr[i+1].ToString()
			values[key] = val
		}

		messages = append(messages, XMessage{
			ID:     msgID,
			Values: values,
		})
	}

	return messages, nil
}

// ReadRange reads a range of messages from a stream without consuming them.
func (v *ValkeyClient) ReadRange(ctx context.Context, streamKey, start, end string, count int64) ([]XMessage, error) {
	var result valkey.ValkeyResult
	if count > 0 {
		result = v.client.Do(ctx, v.client.B().Xrange().
			Key(streamKey).
			Start(start).
			End(end).
			Count(count).
			Build())
	} else {
		result = v.client.Do(ctx, v.client.B().Xrange().
			Key(streamKey).
			Start(start).
			End(end).
			Build())
	}
	if result.Error() != nil {
		return nil, fmt.Errorf("failed to XRANGE messages: %w", result.Error())
	}

	entries, err := result.AsXRange()
	if err != nil {
		return nil, fmt.Errorf("failed to parse XRANGE result: %w", err)
	}

	messages := make([]XMessage, 0, len(entries))
	for _, entry := range entries {
		messages = append(messages, XMessage{
			ID:     entry.ID,
			Values: entry.FieldValues,
		})
	}

	return messages, nil
}

// GetPendingForConsumer returns up to 'count' pending message IDs for a specific consumer
// in the given stream and consumer group.
// It executes: XPENDING <stream> <group> <start> <end> <count> <consumer>
func (v *ValkeyClient) GetPendingForConsumer(ctx context.Context, streamKey, groupName, consumer string, count int64) ([]string, error) {
	if count <= 0 {
		count = 10
	}

	cmd := v.client.B().Xpending().
		Key(streamKey).
		Group(groupName).
		Start("-").
		End("+").
		Count(count).
		Consumer(consumer).
		Build()

	result := v.client.Do(ctx, cmd)
	if result.Error() != nil {
		return nil, fmt.Errorf("failed to get pending for consumer %s: %w", consumer, result.Error())
	}

	// XPENDING with range returns an array of [id, consumer, idle, deliveries]
	arr, err := result.ToArray()
	if err != nil {
		return nil, fmt.Errorf("failed to parse XPENDING entries: %w", err)
	}

	ids := make([]string, 0, len(arr))
	for _, entry := range arr {
		fields, err := entry.ToArray()
		if err != nil || len(fields) < 1 {
			continue
		}
		id, err := fields[0].ToString()
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}

	return ids, nil
}

// DeleteMessages removes messages from a stream.
func (v *ValkeyClient) DeleteMessages(ctx context.Context, streamKey string, messageIDs ...string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	if err := v.client.Do(ctx, v.client.B().Xdel().
		Key(streamKey).
		Id(messageIDs...).
		Build()).Error(); err != nil {
		return fmt.Errorf("failed to delete stream messages: %w", err)
	}
	return nil
}

// AddToDLQ adds a failed message to the dead letter queue
func (v *ValkeyClient) AddToDLQ(ctx context.Context, dlqKey, runID, filepath string, attempts int, reason string) error {
	cmd := v.client.B().Xadd().
		Key(dlqKey).
		Id("*").
		FieldValue().
		FieldValue("run", runID).
		FieldValue("file", filepath).
		FieldValue("attempts", strconv.Itoa(attempts)).
		FieldValue("reason", reason).
		Build()

	err := v.client.Do(ctx, cmd).Error()
	if err != nil {
		return fmt.Errorf("failed to add to DLQ: %w", err)
	}

	return nil
}

// GetConsumerGroupLag returns the lag (unread messages) for a consumer group
func (v *ValkeyClient) GetConsumerGroupLag(ctx context.Context, streamKey, groupName string) (int64, error) {
	cmd := v.client.B().XinfoGroups().Key(streamKey).Build()
	result := v.client.Do(ctx, cmd)

	if result.Error() != nil {
		return 0, fmt.Errorf("failed to get consumer group info: %w", result.Error())
	}

	groups, err := result.ToArray()
	if err != nil {
		return 0, fmt.Errorf("failed to parse group info: %w", err)
	}

	for _, groupVal := range groups {
		groupInfo, err := groupVal.ToMap()
		if err != nil {
			continue
		}

		nameVal, ok := groupInfo["name"]
		if !ok {
			continue
		}

		name, err := nameVal.ToString()
		if err != nil || name != groupName {
			continue
		}

		lagVal, ok := groupInfo["lag"]
		if !ok {
			return 0, nil
		}

		lag, err := lagVal.AsInt64()
		if err != nil {
			return 0, fmt.Errorf("failed to parse lag: %w", err)
		}

		return lag, nil
	}

	return 0, fmt.Errorf("consumer group %s not found", groupName)
}
