uniffi::include_scaffolding!("amo");

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::sync::{Arc, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use futures_util::{SinkExt, StreamExt};
use scylla::{Session, SessionBuilder};

#[derive(Serialize, Deserialize, Clone)]
pub struct Message {
    pub id: String,
    pub text: String,
    pub sender: String,
    pub timestamp: u64,
}

pub struct AmoStore {
    messages: Arc<Mutex<Vec<Message>>>,
}

impl AmoStore {
    pub fn new() -> Self {
        AmoStore {
            messages: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn add_message(&self, text: String, sender: String) -> String {
        let msg = Message {
            id: Uuid::new_v4().to_string(),
            text,
            sender,
            timestamp: 0,
        };
        let id = msg.id.clone();
        self.messages.lock().unwrap().push(msg);
        id
    }

    pub fn get_messages(&self) -> Vec<String> {
        self.messages
            .lock()
            .unwrap()
            .iter()
            .map(|m| serde_json::to_string(m).unwrap())
            .collect()
    }

    pub fn sync_message(&self, server_url: String, text: String, sender: String) -> String {
        let msg = Message {
            id: Uuid::new_v4().to_string(),
            text,
            sender,
            timestamp: 0,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let messages_ref = Arc::clone(&self.messages);

        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let (mut ws_stream, _) = connect_async(&server_url)
                    .await
                    .map_err(|e| e.to_string())?;

                ws_stream
                    .send(WsMessage::Text(json.clone().into()))
                    .await
                    .map_err(|e| e.to_string())?;

                println!("Message sent: {}", json);

                while let Some(incoming) = ws_stream.next().await {
                    match incoming {
                        Ok(WsMessage::Text(raw)) => {
                            println!("Message received: {}", raw);

                            if let Ok(received_msg) = serde_json::from_str::<Message>(&raw) {
                                messages_ref.lock().unwrap().push(received_msg);
                                return Ok::<String, String>(raw.to_string());
                            }
                        }
                        Ok(WsMessage::Close(_)) => {
                            println!("Connection closed");
                            break;
                        }
                        Err(e) => {
                            return Err(e.to_string());
                        }
                        _ => {}
                    }
                }

                Ok::<String, String>("no message received".to_string())
            });

        match result {
            Ok(received) => received,
            Err(e) => format!("error: {}", e),
        }
    }

    pub fn save_to_scylla(&self, node_url: String, text: String, sender: String) -> String {
        let msg = Message {
            id: Uuid::new_v4().to_string(),
            text,
            sender,
            timestamp: 0,
        };

        let msg_id = msg.id.clone();

        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let session: Session = SessionBuilder::new()
                    .known_node(&node_url)
                    .build()
                    .await
                    .map_err(|e| e.to_string())?;

                session.query_unpaged(
                    "CREATE KEYSPACE IF NOT EXISTS amo 
                     WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                    &[],
                )
                .await
                .map_err(|e| e.to_string())?;

                session.query_unpaged(
                    "CREATE TABLE IF NOT EXISTS amo.messages (
                        id text PRIMARY KEY,
                        text text,
                        sender text,
                        timestamp bigint
                    )",
                    &[],
                )
                .await
                .map_err(|e| e.to_string())?;

                session.query_unpaged(
                    "INSERT INTO amo.messages (id, text, sender, timestamp) VALUES (?, ?, ?, ?)",
                    (&msg.id, &msg.text, &msg.sender, 0i64),
                )
                .await
                .map_err(|e| e.to_string())?;

                Ok::<String, String>(msg_id)
            });

        match result {
            Ok(id) => format!("Message saved to ScyllaDB with ID: {}", id),
            Err(e) => format!("error: {}", e),
        }
    }
}

pub fn greet(name: String) -> String {
    format!("Hello, {}! AmoCore is running.", name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_get_message() {
        let store = AmoStore::new();
        let id = store.add_message("Hello amo!".to_string(), "Aakanksha".to_string());

        let messages = store.get_messages();

        assert!(!messages.is_empty());
        assert!(messages[0].contains("Hello amo!"));
        assert!(messages[0].contains("Aakanksha"));

        println!("Message added with ID: {}", id);
        println!("Messages in store: {:?}", messages);
    }

    #[test]
    fn test_multiple_messages() {
        let store = AmoStore::new();

        store.add_message("First message".to_string(), "Aakanksha".to_string());
        store.add_message("Second message".to_string(), "Bob".to_string());
        store.add_message("Third message".to_string(), "Alice".to_string());

        let messages = store.get_messages();

        assert_eq!(messages.len(), 3);
        println!("All 3 messages stored correctly");
        println!("Messages: {:?}", messages);
    }

    #[test]
    fn test_unique_ids() {
        let store = AmoStore::new();

        let id1 = store.add_message("Message 1".to_string(), "Aakanksha".to_string());
        let id2 = store.add_message("Message 2".to_string(), "Aakanksha".to_string());
        let id3 = store.add_message("Message 3".to_string(), "Aakanksha".to_string());

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);

        println!("All IDs are unique");
        println!("ID1: {}", id1);
        println!("ID2: {}", id2);
        println!("ID3: {}", id3);
    }

    #[test]
    fn test_message_serialization() {
        let store = AmoStore::new();
        store.add_message("Test JSON".to_string(), "Aakanksha".to_string());

        let messages = store.get_messages();
        let parsed: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();

        assert!(parsed["id"].is_string());
        assert!(parsed["text"].is_string());
        assert!(parsed["sender"].is_string());
        assert_eq!(parsed["text"], "Test JSON");
        assert_eq!(parsed["sender"], "Aakanksha");

        println!("Message serializes to valid JSON");
        println!("JSON: {}", messages[0]);
    }

    #[test]
    fn test_websocket_sync() {
        let store = AmoStore::new();

        let result = store.sync_message(
            "wss://echo.websocket.org".to_string(),
            "Hello from amo sync engine!".to_string(),
            "Aakanksha".to_string(),
        );

        println!("WebSocket result: {}", result);

        let messages = store.get_messages();
        println!("Messages after sync: {:?", messages);
    }
}