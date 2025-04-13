"""
Models for interacting with Cassandra tables in the Facebook Messenger backend project.
"""
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from app.db.cassandra_client import cassandra_client
import logging
from cassandra.query import SimpleStatement
  
logger = logging.getLogger(__name__)
 
class MessageModel:
    """
    Message model for interacting with the messages table.
    """
 
    @staticmethod
    async def create_message(conversation_id: int, sender_id: int, receiver_id: int, content: str) -> Dict[str, Any]:
        """
        Create a new message.
 
        Args:
            sender_id (int): ID of the sender
            receiver_id (int): ID of the receiver
            content (str): Content of the message
 
        Returns:
            dict: Details of the created message matching MessageResponse schema
        """ 
 
        # Get the next message ID
        message_id_queries = "SELECT counter_value FROM counter WHERE counter_name = 'message_id'"
        value = await cassandra_client.execute(message_id_queries)
        logger.info(f"Value fetched: {value}")
        message_id = value[0]["counter_value"] + 1 if value else 1
        logger.info(f"Message ID: {message_id}")
        # Update the counter
        await cassandra_client.execute(
            "UPDATE counter SET counter_value = counter_value + 1 WHERE counter_name = 'message_id'"
        )
 
        created_at = datetime.now()
 
        # Insert into messages table
        queries = """
        INSERT INTO messages (message_id, conversation_id, sender_id, receiver_id, content, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        await cassandra_client.execute(queries, (message_id, conversation_id, sender_id, receiver_id, content, created_at))
 
        check_conversation_queries = """
            select conversation_id from user_conversations where conversation_id = %s 
            """
        values = await cassandra_client.execute(check_conversation_queries, (conversation_id,))
        for row in values:
            logger.info(f"Row: {row}")
        if not values:
            logger.info("No existing conversation found.")
        else:
            for value in values:
                logger.info(f"Checking if conversation already exists: {value}")
 
        if not values:
            # If conversation doesn't exist, create it
            insert_queries = """
            INSERT INTO user_conversations (conversation_id, sender_id, receiver_id, last_timestamp, last_message)
            VALUES (%s, %s, %s, %s, %s)
            """
            await cassandra_client.execute(insert_queries, (conversation_id, sender_id, receiver_id, created_at, content))
        else:
            # If conversation exists, update it with the new message
            update_queries = """
            UPDATE user_conversations SET last_timestamp = %s, last_message = %s, sender_id = %s, receiver_id = %s WHERE conversation_id = %s
            """
            await cassandra_client.execute(update_queries, (created_at, content, sender_id, receiver_id, conversation_id))
 
 
        return {
            "message_id": message_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content,
            "timestamp": created_at,
            "conversation_id": conversation_id
        }












    @staticmethod
    async def get_conversation_messages(conversation_id: int, page: int = 1, limit: int = 20) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get messages for a conversation with pagination.
 
        Args:
            conversation_id (int): ID of the conversation
            page (int): Page number for pagination (default: 1)
            limit (int): Number of messages per page (default: 20)
 
        Returns:
            tuple: (List of messages, Total count) for PaginatedMessageResponse
        """
        # Get total count of messages in the conversation
        count_queries = """
        SELECT COUNT(*) as count FROM messages WHERE conversation_id = %s
        """
        count_results = await cassandra_client.execute(count_queries, (conversation_id,))
        total = count_results[0]["count"] if count_results else 0
  
        # Get messages with pagination
        queries = """
        SELECT message_id, sender_id, receiver_id, content, timestamp
        FROM messages
        WHERE conversation_id = %s
        ORDER BY timestamp DESC 
        """
 
        values = await cassandra_client.execute(queries, (conversation_id,))
 
        messages = []
        for value in values:
            messages.append({
                "id": value["message_id"],
                "sender_id": value["sender_id"],
                "receiver_id": value["receiver_id"],
                "content": value["content"],
                "created_at": value["timestamp"],
                "conversation_id": conversation_id
            })
        total = len(messages)
        offset = (page - 1) * limit
        paginated_messages = messages[offset:offset + limit]
 
        messages = paginated_messages if paginated_messages else []
        return messages, total







    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: int, 
        before_timestamp: datetime, 
        page: int = 1, 
        limit: int = 20
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get messages before a timestamp with pagination.
 
        Args:
            conversation_id (int): ID of the conversation
            before_timestamp (datetime): Timestamp to filter messages
            page (int): Page number for pagination (default: 1)
            limit (int): Number of messages per page (default: 20)
 
        Returns:
            tuple: (List of messages, Total count) for PaginatedMessageResponse
        """
        # Get total count of messages before the timestamp
        count_queries = """
        SELECT COUNT(*) as count FROM messages 
        WHERE conversation_id = %s AND timestamp < %s
        """
        count_results = await cassandra_client.execute(count_queries, (conversation_id, before_timestamp))
        total = count_results[0]["count"] if count_results else 0
 
        # Get messages before timestamp with pagination
        queries = """
        SELECT message_id, sender_id, receiver_id, content, timestamp
        FROM messages
        WHERE conversation_id = %s AND timestamp < %s
        ORDER BY timestamp DESC
        """
        values = await cassandra_client.execute(queries, (conversation_id, before_timestamp))
 
        messages = []
        for value in values:
            messages.append({
                "id": value["message_id"],
                "sender_id": value["sender_id"],
                "receiver_id": value["receiver_id"],
                "content": value["content"],
                "created_at": value["timestamp"],
                "conversation_id": conversation_id
            })
 
        total = len(messages)
        offset = (page - 1) * limit
        paginated_messages = messages[offset:offset + limit]
 
        messages = paginated_messages if paginated_messages else []
 
        return messages, total
 
 
class ConversationModel:
    """
    Conversation model for interacting with the conversation-related tables.
    """
 
    @staticmethod
    async def get_user_conversations(user_id: int, page: int = 1, limit: int = 20) -> Tuple[List[Dict[str, Any]], int]:
 
        query_1 = """
        SELECT conversation_id, sender_id, receiver_id, last_timestamp, last_message
        FROM user_conversations 
        WHERE sender_id = %s 
        ALLOW FILTERING       
        """
 
        query_2 = """
        SELECT conversation_id, sender_id, receiver_id, last_timestamp, last_message
        FROM user_conversations 
        WHERE receiver_id = %s
        ALLOW FILTERING
        """

        values1 = await cassandra_client.execute(query_2, (user_id,))
        values = await cassandra_client.execute(query_1, (user_id,))
        values_list = list(values)
        values_list2 = list(values1)
        values_list.extend(values_list2)
        sorted(values_list, key=lambda x: x["last_timestamp"])
 
        logger.info(f"Values fetched: {values_list}")
 
        total = len(values_list)
        offset = (page - 1) * limit
        paginated_values = values_list[offset:offset + limit]
 
        conversation = []
 
        for values in paginated_values:
            logger.info(f"Checking if conversation already exists: {values}")
            conversation.append({
                "id": values["conversation_id"],
                "user1_id": values["sender_id"],
                "user2_id": values["receiver_id"],
                "last_message_at": values["last_timestamp"],
                "last_message_content": values["last_message"]
            }) 
 
 
 
        return conversation, total






    @staticmethod
    async def create_conversation(sender_id: int, receiver_id: int):
        try:
           # getting the conversation id by running the counter
            conversation_id_queries = "SELECT counter_value FROM counter WHERE counter_name = 'conversation_id'"
            result = await cassandra_client.execute(conversation_id_queries)
            conversation_id = result[0]["counter_value"] + 1 if result else 1
 
            # Update the counter
            await cassandra_client.execute(
                "UPDATE counter SET counter_value = counter_value + 1 WHERE counter_name = 'conversation_id'"
            )
 
            created_at = datetime.now()
 
            insert_queries = """
                            INSERT INTO conversation (conversation_id, sender_id, receiver_id, last_timestamp)
                            VALUES (%s, %s, %s, %s)
            """
 
            await cassandra_client.execute(insert_queries, (conversation_id, sender_id, receiver_id, created_at))
 
            return {
                "conversation_id": conversation_id,
                "sender_id": sender_id,
                "receiver_id": receiver_id,
                "created_at": created_at
            }
 
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Database error: {str(e)}"
            )
 



    @staticmethod
    async def get_conversation(conversation_id: int) -> Dict[str, Any]:
        """
        Get a conversation by ID.
 
        Args:
            conversation_id (int): ID of the conversation
 
        Returns:
            dict: Details of the conversation matching ConversationResponse schema
        """
        queries = """
                SELECT conversation_id, sender_id, receiver_id, last_timestamp, last_message
                FROM user_conversations 
                WHERE conversation_id = %s
                """
        values = await cassandra_client.execute(queries, (conversation_id,))
 
        if not values:
            return None
 
        value = values[0]
 
 
        return {
            "conversation_id": value["conversation_id"],
            "sender_id": value["sender_id"],
            "receiver_id": value["receiver_id"],
            "last_message_at": value["last_timestamp"],
            "last_message_content": value["last_message"]
        }





    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int) -> Dict[str, Any]:
        """
        Get an existing conversation between two users or create a new one.
 
        Args:
            user1_id (int): ID of the first user
            user2_id (int): ID of the second user
 
        Returns:
            dict: Details of the conversation matching ConversationResponse schema
        """
 
        # Check if the conversation already exists
        query_1 = """
                SELECT conversation_id FROM conversation
                WHERE sender_id = %s AND receiver_id = %s 
                ALLOW FILTERING
                """
        values1 = await cassandra_client.execute(query_1, (user1_id, user2_id))
 
        query_2 = """
                SELECT conversation_id FROM conversation
                WHERE sender_id = %s AND receiver_id = %s 
                ALLOW FILTERING
                """
        values2 = await cassandra_client.execute(query_2, (user2_id, user1_id))

        for row in values1:
            logger.info(f"Row: {row}")
        for row in values2:
            logger.info(f"Row: {row}")
 
        if values1:
            # If conversation exists, get its details
            return await ConversationModel.get_conversation(values1[0]["conversation_id"])
        if values2:
            # If conversation exists, get its details
            return await ConversationModel.get_conversation(values2[0]["conversation_id"])
 
        # If conversation doesn't exist, create a new one
        # Get the next conversation ID
        conversation_id_queries = "SELECT counter_value FROM counter WHERE counter_name = 'conversation_id'"
        value = await cassandra_client.execute(conversation_id_queries)
        conversation_id = value[0]["counter_value"] + 1 if value else 1
 
        # Update the counter
        await cassandra_client.execute(
            "UPDATE counter SET counter_value = counter_value + 1 WHERE counter_name = 'conversation_id'"
        )
 
        created_at = datetime.now()
 
        # Insert into conversation table
        insert_queries = """
        INSERT INTO conversation (conversation_id, sender_id, receiver_id, last_timestamp)
        VALUES (%s, %s, %s, %s)
        """
        await cassandra_client.execute(insert_queries, (conversation_id, user1_id, user2_id, created_at))
 
 
        # Return conversation details in the format expected by ConversationResponse
        return {
            "conversation_id": conversation_id,
            "sender_id": user1_id,
            "receiver_id": user2_id,
            "last_message_at": created_at,
            "last_message_content": None
        }
