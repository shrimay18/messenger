# Cassandra Schema for Messenger Application

This schema defines the data model for a simple messaging platform where users can send and receive messages. The design supports use cases such as:

- Sending messages between users.
- Storing and retrieving conversation history.
- Fetching recent conversations for a user.
- Paginating messages based on timestamp.

---

## Keyspace

### `messenger`

The Cassandra keyspace used to logically group all messaging-related tables.

```sql
CREATE KEYSPACE IF NOT EXISTS messenger 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

> **Note**: `SimpleStrategy` is used for development/testing. For production, `NetworkTopologyStrategy` is recommended.

---

## Tables

### 1. `counter`

A utility table used to generate unique incremental IDs, such as `conversation_id` or `message_id`.

| Column         | Type     | Description                          |
|----------------|----------|--------------------------------------|
| counter_name   | TEXT     | Unique name for the counter (PK)     |
| counter_value  | COUNTER  | Auto-incrementing value              |

```sql
CREATE TABLE IF NOT EXISTS counter (
    counter_name TEXT,
    counter_value COUNTER,
    PRIMARY KEY (counter_name)
);
```

> This table helps ensure ID uniqueness in a distributed environment using Cassandra's native `COUNTER` type.

---

### 2. `messages`

Stores all individual messages for each conversation. Messages are ordered by timestamp in descending order so that the most recent messages appear first when queried.

| Column          | Type      | Description                             |
|------------------|-----------|-----------------------------------------|
| conversation_id  | INT       | Unique conversation ID (Partition Key)  |
| timestamp        | TIMESTAMP | Time the message was sent               |
| message_id       | INT       | Message sequence number or ID           |
| content          | TEXT      | Message body                            |
| sender_id        | INT       | ID of the sender                        |
| receiver_id      | INT       | ID of the receiver                      |

```sql
CREATE TABLE IF NOT EXISTS messages (
    conversation_id INT,
    timestamp TIMESTAMP,
    message_id INT,
    content TEXT,
    sender_id INT,
    receiver_id INT,
    PRIMARY KEY (conversation_id, timestamp, message_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC);
```

> Supports features like:
> - Fetching entire conversation history.
> - Retrieving messages in reverse chronological order.
> - Paginating older messages based on timestamp.

---

### 3. `user_conversations`

Tracks the latest message exchanged in each conversation for displaying recent chats.

| Column           | Type      | Description                               |
|------------------|-----------|-------------------------------------------|
| sender_id        | INT       | ID of the sender of the last message      |
| receiver_id      | INT       | ID of the receiver of the last message    |
| conversation_id  | INT       | Unique conversation ID (Primary Key)      |
| last_timestamp   | TIMESTAMP | Time of the most recent message           |
| last_message     | TEXT      | Content of the most recent message        |

```sql
CREATE TABLE IF NOT EXISTS user_conversations (
    sender_id INT,
    receiver_id INT,
    conversation_id INT,
    last_timestamp TIMESTAMP,
    last_message TEXT,
    PRIMARY KEY (conversation_id)
);
```

> This table is ideal for:
> - Showing a list of active chats.
> - Quickly accessing the last message preview and time for UI display.

---

### 4. `conversation`

Stores a high-level summary of each conversation with sender-specific partitioning.

| Column           | Type      | Description                               |
|------------------|-----------|-------------------------------------------|
| conversation_id  | INT       | Unique conversation ID (Partition Key)    |
| sender_id        | INT       | ID of the sender (Clustering Key)         |
| receiver_id      | INT       | ID of the receiver                        |
| last_timestamp   | TIMESTAMP | Timestamp of last activity                |

```sql
CREATE TABLE IF NOT EXISTS conversation (
    conversation_id INT,
    sender_id INT,
    receiver_id INT,
    last_timestamp TIMESTAMP,
    PRIMARY KEY (conversation_id, sender_id)
);
```

> Useful for:
> - Tracking conversation ownership and participant pairs.
> - Filtering conversations by sender efficiently.

---

## Design Highlights

- **Scalability**: The schema supports horizontal scaling by partitioning data using `conversation_id`.
- **Performance**: Tables are designed with proper primary keys and clustering columns to optimize read queries like fetching messages and conversations.
- **Read-Optimized**: Since Cassandra is a read-efficient, write-heavy NoSQL DB, this schema suits real-time messaging use cases well.

---

## Future Considerations

- Add secondary indexes or materialized views for features like searching users or conversations.
- Store message read/unread status in a separate table if needed.
- Implement access controls if the app grows to multiple user types (admin, moderator, etc.).

---

```bash
# To initialize the schema, run the provided Python setup script.
python cassandra_init.py
```
