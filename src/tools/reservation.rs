use std::collections::HashMap;
use std::sync::Arc;

use ai_agents::tools::{
    Tool, ToolExecutionContext, ToolOperationKind, ToolResult, ToolSafetyMetadata,
    ToolSideEffectLevel,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestaurantOption {
    pub id: String,
    pub name: String,
    pub area: String,
    pub available_times: Vec<String>,
    pub parking: bool,
    pub window_seat: bool,
    pub maximum_party_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReservationRecord {
    pub confirmation_id: String,
    pub restaurant_id: String,
    pub date: String,
    pub time: String,
    pub party_size: u32,
    pub customer_name: String,
    pub window_seat_requested: bool,
}

#[derive(Default)]
pub struct ReservationStore {
    restaurants: Vec<RestaurantOption>,
    reservations_by_key: HashMap<String, ReservationRecord>,
}

impl ReservationStore {
    pub fn demo() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            restaurants: vec![
                RestaurantOption {
                    id: "gangnam-table".into(),
                    name: "강남 테이블".into(),
                    area: "강남".into(),
                    available_times: vec!["18:30".into(), "19:00".into(), "20:00".into()],
                    parking: true,
                    window_seat: true,
                    maximum_party_size: 8,
                },
                RestaurantOption {
                    id: "hongdae-kitchen".into(),
                    name: "홍대 키친".into(),
                    area: "홍대".into(),
                    available_times: vec!["19:00".into(), "19:30".into()],
                    parking: false,
                    window_seat: true,
                    maximum_party_size: 6,
                },
                RestaurantOption {
                    id: "seongsu-dining".into(),
                    name: "성수 다이닝".into(),
                    area: "성수".into(),
                    available_times: vec!["18:00".into(), "19:00".into(), "20:30".into()],
                    parking: true,
                    window_seat: false,
                    maximum_party_size: 10,
                },
            ],
            reservations_by_key: HashMap::new(),
        }))
    }
}

pub struct SearchAvailabilityTool {
    store: Arc<Mutex<ReservationStore>>,
}

impl SearchAvailabilityTool {
    pub fn new(store: Arc<Mutex<ReservationStore>>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Tool for SearchAvailabilityTool {
    fn id(&self) -> &str {
        "search_availability"
    }

    fn name(&self) -> &str {
        "Search reservation availability"
    }

    fn description(&self) -> &str {
        "Search demo restaurants by area, time, party size, parking, and window seat preference. This tool is read-only."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "area": { "type": "string" },
                "date": { "type": "string" },
                "time": { "type": "string" },
                "party_size": { "type": "integer", "minimum": 1 },
                "parking_required": { "type": "boolean", "default": false },
                "window_seat_preferred": { "type": "boolean", "default": false }
            },
            "required": ["date", "time", "party_size"]
        })
    }

    async fn execute(&self, args: Value, _ctx: ToolExecutionContext) -> ToolResult {
        let area = args.get("area").and_then(Value::as_str);
        let Some(time) = args.get("time").and_then(Value::as_str) else {
            return ToolResult::error("time is required");
        };
        let Some(party_size) = args.get("party_size").and_then(Value::as_u64) else {
            return ToolResult::error("party_size is required");
        };
        let parking_required = args
            .get("parking_required")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let window_preferred = args
            .get("window_seat_preferred")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let store = self.store.lock();
        let mut matches: Vec<RestaurantOption> = store
            .restaurants
            .iter()
            .filter(|restaurant| {
                area.is_none_or(|area| restaurant.area == area)
                    && restaurant.available_times.iter().any(|slot| slot == time)
                    && restaurant.maximum_party_size >= party_size as u32
                    && (!parking_required || restaurant.parking)
            })
            .cloned()
            .collect();

        if window_preferred {
            matches.sort_by_key(|restaurant| !restaurant.window_seat);
        }

        ToolResult::ok(
            serde_json::to_string(&json!({
                "date": args.get("date").cloned().unwrap_or(Value::Null),
                "time": time,
                "party_size": party_size,
                "options": matches,
            }))
            .unwrap_or_else(|_| "{\"options\":[]}".into()),
        )
    }

    fn safety_metadata(&self) -> ToolSafetyMetadata {
        ToolSafetyMetadata::read_only(ToolOperationKind::Read)
    }
}

pub struct ReserveRestaurantTool {
    store: Arc<Mutex<ReservationStore>>,
}

impl ReserveRestaurantTool {
    pub fn new(store: Arc<Mutex<ReservationStore>>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Tool for ReserveRestaurantTool {
    fn id(&self) -> &str {
        "reserve_restaurant"
    }

    fn name(&self) -> &str {
        "Reserve a restaurant"
    }

    fn description(&self) -> &str {
        "Create one demo reservation after the user explicitly confirms every detail. An idempotency key is required and repeated calls return the original record."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "restaurant_id": { "type": "string" },
                "date": { "type": "string" },
                "time": { "type": "string" },
                "party_size": { "type": "integer", "minimum": 1 },
                "customer_name": { "type": "string" },
                "window_seat_requested": { "type": "boolean", "default": false },
                "confirmed": { "type": "boolean", "description": "True only after explicit user confirmation" },
                "idempotency_key": { "type": "string" }
            },
            "required": [
                "restaurant_id", "date", "time", "party_size",
                "customer_name", "confirmed", "idempotency_key"
            ]
        })
    }

    async fn execute(&self, args: Value, _ctx: ToolExecutionContext) -> ToolResult {
        if args.get("confirmed").and_then(Value::as_bool) != Some(true) {
            return ToolResult::error("explicit user confirmation is required");
        }

        let required_string = |name: &str| {
            args.get(name)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| format!("{name} is required"))
        };

        let restaurant_id = match required_string("restaurant_id") {
            Ok(value) => value,
            Err(error) => return ToolResult::error(error),
        };
        let date = match required_string("date") {
            Ok(value) => value,
            Err(error) => return ToolResult::error(error),
        };
        let time = match required_string("time") {
            Ok(value) => value,
            Err(error) => return ToolResult::error(error),
        };
        let customer_name = match required_string("customer_name") {
            Ok(value) => value,
            Err(error) => return ToolResult::error(error),
        };
        let idempotency_key = match required_string("idempotency_key") {
            Ok(value) => value,
            Err(error) => return ToolResult::error(error),
        };
        let Some(party_size) = args.get("party_size").and_then(Value::as_u64) else {
            return ToolResult::error("party_size is required");
        };

        let mut store = self.store.lock();

        if let Some(existing) = store.reservations_by_key.get(&idempotency_key) {
            return ToolResult::ok(serde_json::to_string(existing).unwrap_or_else(|_| "{}".into()));
        }

        let Some(restaurant) = store
            .restaurants
            .iter()
            .find(|restaurant| restaurant.id == restaurant_id)
        else {
            return ToolResult::error("restaurant_id was not found");
        };

        if !restaurant.available_times.iter().any(|slot| slot == &time) {
            return ToolResult::error("the selected time is unavailable");
        }
        if restaurant.maximum_party_size < party_size as u32 {
            return ToolResult::error("party size exceeds restaurant capacity");
        }

        let record = ReservationRecord {
            confirmation_id: format!("DEMO-{}", uuid::Uuid::new_v4().simple()),
            restaurant_id,
            date,
            time,
            party_size: party_size as u32,
            customer_name,
            window_seat_requested: args
                .get("window_seat_requested")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };

        store
            .reservations_by_key
            .insert(idempotency_key, record.clone());

        ToolResult::ok(serde_json::to_string(&record).unwrap_or_else(|_| "{}".into()))
    }

    fn safety_metadata(&self) -> ToolSafetyMetadata {
        let mut metadata = ToolSafetyMetadata::conservative_unknown();
        metadata.operation = ToolOperationKind::Write;
        metadata.side_effect_level = ToolSideEffectLevel::ExternalWrite;
        metadata.host_dependent = true;
        metadata.supports_cancellation = true;
        metadata.default_requires_approval = false;
        metadata.max_output_chars = Some(4_000);
        metadata.max_result_size_chars = Some(4_000);
        metadata
    }
}

pub fn task_tools(store: Arc<Mutex<ReservationStore>>) -> Vec<Arc<dyn Tool>> {
    vec![
        Arc::new(SearchAvailabilityTool::new(Arc::clone(&store))),
        Arc::new(ReserveRestaurantTool::new(store)),
    ]
}

pub fn speculative_tools(store: Arc<Mutex<ReservationStore>>) -> Vec<Arc<dyn Tool>> {
    vec![Arc::new(SearchAvailabilityTool::new(store))]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> ToolExecutionContext {
        ToolExecutionContext::test("reservation-test")
    }

    #[tokio::test]
    async fn search_filters_pure_read_results() {
        let store = ReservationStore::demo();
        let tool = SearchAvailabilityTool::new(store);

        let result = tool
            .execute(
                json!({
                    "area": "강남",
                    "date": "2026-09-26",
                    "time": "19:00",
                    "party_size": 4,
                    "parking_required": true
                }),
                context(),
            )
            .await;

        assert!(result.success);
        assert!(result.output.contains("gangnam-table"));
        assert!(!result.output.contains("hongdae-kitchen"));
        assert!(tool.safety_metadata().read_only);
    }

    #[tokio::test]
    async fn reservation_requires_confirmation_and_is_idempotent() {
        let store = ReservationStore::demo();
        let tool = ReserveRestaurantTool::new(store);
        let args = json!({
            "restaurant_id": "gangnam-table",
            "date": "2026-09-26",
            "time": "19:00",
            "party_size": 4,
            "customer_name": "홍길동",
            "confirmed": true,
            "idempotency_key": "turn-1-reservation"
        });

        let first = tool.execute(args.clone(), context()).await;
        let second = tool.execute(args, context()).await;

        assert!(first.success);
        assert_eq!(first.output, second.output);

        let rejected = tool
            .execute(
                json!({
                    "restaurant_id": "gangnam-table",
                    "date": "2026-09-26",
                    "time": "19:00",
                    "party_size": 4,
                    "customer_name": "홍길동",
                    "confirmed": false,
                    "idempotency_key": "turn-2-reservation"
                }),
                context(),
            )
            .await;
        assert!(!rejected.success);
    }
}
