use super::*;

#[async_trait]
impl Sut for DatabaseSut {
    async fn apply(&mut self, input: &Input) -> Vec<Observation> {
        if self.symbolic.is_some() { self.apply_symbolic(input).await } else { self.play(input).await }
    }

    /// Closes the database a trace left open, so the next trace can open its own.
    async fn finish(&mut self) {
        self.mock.release_all();
        if let Some(database) = self.database.take() { let _ = database.close_database().await; }
        self.remove_file_dir();
    }
}

impl DatabaseSut {
    /// One input of the vocabulary on the plain stand.
    pub(super) async fn play(&mut self, input: &Input) -> Vec<Observation> {
        match input.name.as_str() {
            "open" => return self.open(input.text("models").unwrap_or(""), input.flag("encrypted").unwrap_or(false), input.text("path").map(str::to_string)).await,
            "clock_back" => {
                self.back += input.number("ms").unwrap_or(0);
                return Vec::new();
            }
            "tick" => {
                self.clock.advance(input.number("ms").unwrap_or(0));
                self.clock_now += input.number("ms").unwrap_or(0);
                return self.settle().await;
            }
            "answer" | "error" | "drop" | "ready" | "frame" => {
                let Some(id) = input.text("id") else { return error("arguments") };
                let reached = match input.name.as_str() {
                    "answer" => input.args.get("json").is_some_and(|body| self.mock.answer(id, &body.to_string())),
                    "error" => input.text("kind").is_some_and(|kind| self.mock.fail(id, kind)),
                    "drop" => self.mock.drop_connection(id),
                    "ready" => self.mock.ready(id),
                    _ => match input.args.get("event") {
                        Some(event) => match self.mock.frame(id, &event.to_string()) {
                            Ok(reached) => reached,
                            Err(_) => return error("frame"),
                        },
                        None => return error("arguments"),
                    },
                };
                if !reached { return error("no_request"); }
                return self.settle().await;
            }
            "lost" => return vec![Observation::new("unsupported", json!(input.name))],
            _ => {}
        }
        let Some(database) = self.database.clone() else { return error("not_open") };
        let mut observed = match input.name.as_str() {
            "online" => match database.online_sink() {
                Ok(sink) => { sink.set_online(input.flag("online").unwrap_or(false)); Vec::new() }
                Err(_) => error("online"),
            },
            "set_online" => match database.set_online(input.flag("online").unwrap_or(false)) {
                Ok(()) => vec![Observation::new("online", json!(input.flag("online").unwrap_or(false)))],
                Err(_) => error("online"),
            },
            "published" => vec![Observation::new("published", json!({ "epoch": database.session_epoch() }))],
            "open_list" | "open_count" | "open_entity" | "open_events" => self.open_stream(&database, input),
            "pull" => {
                let Some(name) = input.text("stream") else { return error("arguments") };
                let Some(pulled) = self.streams.get(name) else { return error("unknown_stream") };
                let unread: Vec<ReadValue> = pulled.queue.lock().unwrap().drain(..).map(|(_, value)| value).collect();
                if unread.is_empty() { vec![Observation::new("empty", json!({ "stream": name }))] } else { fold::fold(unread).iter().map(|folded| self.folded(name, folded)).collect() }
            }
            "cancel" => {
                let Some(target) = input.text("target") else { return error("arguments") };
                if let Some(cancel) = self.loads.get(target) { cancel(); } else if let Some(pulled) = self.streams.get(target) { let stream = pulled.stream.clone(); self.launch(async move { let _ = stream.cancel().await; Done::default() }); } else { return error("unknown_target"); }
                Vec::new()
            }
            _ => match self.start(database, input) {
                Some(immediate) => immediate,
                None => return vec![Observation::new("unsupported", json!(input.name))],
            },
        };
        observed.extend(self.settle().await);
        if input.name == "close" && self.calls.is_empty() { self.database = None; }
        observed
    }
}
