package com.noma4i.dblayer.models

import com.noma4i.dblayer.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

@Serializable
data class Chat(
    override val id: String,
    val title: String,
    val lastMessageAt: String? = null,
    val lastMessageId: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Chat"
    companion object {
        val title = Column<Chat, String>("title")
        val lastMessageAt = Column<Chat, String?>("lastMessageAt")
        val lastMessageId = Column<Chat, String?>("lastMessageId")
        fun find(id: String) = Query("Chat", serializer()).find(id)
        fun where(condition: Condition) = Query("Chat", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("Chat", serializer(), ids)
        fun entity(id: String) = entityOf("Chat", serializer(), id)
        val recent: Query<Chat> get() {
            var built = Query("Chat", serializer(), relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("lastMessageAt", "desc"))
            return built
        }
        suspend fun create(title: String, lastMessageAt: String? = null, lastMessageId: String? = null): Chat {
            val values = buildJsonObject {
                put("title", jsonValue(title))
                lastMessageAt?.let { put("lastMessageAt", jsonValue(it)) }
                lastMessageId?.let { put("lastMessageId", jsonValue(it)) }
            }
            return dbJson.decodeFromString(serializer(), DBlayer.database.create("Chat", values.toString()))
        }
    }
    suspend fun update(title: String? = null, lastMessageAt: String? = null, lastMessageId: String? = null): Chat {
        val values = buildJsonObject {
            title?.let { put("title", jsonValue(it)) }
            lastMessageAt?.let { put("lastMessageAt", jsonValue(it)) }
            lastMessageId?.let { put("lastMessageId", jsonValue(it)) }
        }
        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values.toString()))
    }
    val messages get() = related("messages", Message.serializer())
    val lastMessage get() = related("lastMessage", Message.serializer())
}

@Serializable
data class Message(
    override val id: String,
    val chatId: String,
    val authorId: String,
    val text: String,
    val createdAt: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Message"
    companion object {
        val chatId = Column<Message, String>("chatId")
        val authorId = Column<Message, String>("authorId")
        val text = Column<Message, String>("text")
        val createdAt = Column<Message, String>("createdAt")
        fun find(id: String) = Query("Message", serializer()).find(id)
        fun where(condition: Condition) = Query("Message", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("Message", serializer(), ids)
        fun entity(id: String) = entityOf("Message", serializer(), id)
        fun byChat(chatId: String): Query<Message> {
            var built = Query("Message", serializer(), relation = "by_chat", params = buildJsonObject { put("chatId", jsonValue(chatId)) })
            built = built.where(Condition.equal("chatId", jsonValue(chatId)))
            built = built.order(Order("createdAt", "asc"))
            return built
        }
        val history: Query<Message> get() {
            var built = Query("Message", serializer(), relation = "history", params = buildJsonObject {  })
            return built
        }
        suspend fun create(chatId: String, authorId: String, text: String): Message {
            val values = buildJsonObject {
                put("chatId", jsonValue(chatId))
                put("authorId", jsonValue(authorId))
                put("text", jsonValue(text))
            }
            return dbJson.decodeFromString(serializer(), DBlayer.database.create("Message", values.toString()))
        }
    }
    suspend fun update(chatId: String? = null, authorId: String? = null, text: String? = null, createdAt: String? = null): Message {
        val values = buildJsonObject {
            chatId?.let { put("chatId", jsonValue(it)) }
            authorId?.let { put("authorId", jsonValue(it)) }
            text?.let { put("text", jsonValue(it)) }
            createdAt?.let { put("createdAt", jsonValue(it)) }
        }
        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values.toString()))
    }
    val chat get() = related("chat", Chat.serializer())
}

@Serializable
data class Moment(
    override val id: String,
    val uuid: String,
    val userId: String? = null,
    val text: String? = null,
    val likedByMe: Boolean,
    val media: JsonElement,
    val tone: String? = null,
    val createdAt: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Moment"
    companion object {
        val uuid = Column<Moment, String>("uuid")
        val userId = Column<Moment, String?>("userId")
        val text = Column<Moment, String?>("text")
        val likedByMe = Column<Moment, Boolean>("likedByMe")
        val media = Column<Moment, JsonElement>("media")
        val tone = Column<Moment, String?>("tone")
        val createdAt = Column<Moment, String>("createdAt")
        fun find(id: String) = Query("Moment", serializer()).find(id)
        fun where(condition: Condition) = Query("Moment", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("Moment", serializer(), ids)
        fun entity(id: String) = entityOf("Moment", serializer(), id)
        val recent: Query<Moment> get() {
            var built = Query("Moment", serializer(), relation = "recent", params = buildJsonObject {  })
            built = built.order(Order("createdAt", "desc"))
            return built
        }
        fun byUser(userId: String?): Query<Moment> {
            var built = Query("Moment", serializer(), relation = "by_user", params = buildJsonObject { put("userId", jsonValue(userId)) })
            built = built.where(Condition.equal("userId", jsonValue(userId)))
            built = built.order(Order("createdAt", "desc"))
            return built
        }
        val feed: Query<Moment> get() {
            var built = Query("Moment", serializer(), relation = "feed", params = buildJsonObject {  })
            return built
        }
        suspend fun create(text: String? = null, media: JsonElement? = null): Moment {
            val values = buildJsonObject {
                text?.let { put("text", jsonValue(it)) }
                media?.let { put("media", jsonValue(it)) }
            }
            return dbJson.decodeFromString(serializer(), DBlayer.database.create("Moment", values.toString()))
        }
    }
    suspend fun update(uuid: String? = null, userId: String? = null, text: String? = null, likedByMe: Boolean? = null, media: JsonElement? = null, createdAt: String? = null): Moment {
        val values = buildJsonObject {
            uuid?.let { put("uuid", jsonValue(it)) }
            userId?.let { put("userId", jsonValue(it)) }
            text?.let { put("text", jsonValue(it)) }
            likedByMe?.let { put("likedByMe", jsonValue(it)) }
            media?.let { put("media", jsonValue(it)) }
            createdAt?.let { put("createdAt", jsonValue(it)) }
        }
        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values.toString()))
    }
    suspend fun like(): Moment = act("like", JsonObject(parseJsonObject("""{"likedByMe":true}""") + mapOf()), JsonObject(mapOf()), serializer())
    suspend fun share(): ActionAnswer<Moment, MomentShareResult> = actAnswering("share", JsonObject(parseJsonObject("""{}""") + mapOf()), JsonObject(mapOf()), serializer(), MomentShareResult.serializer())
    val author get() = related("author", User.serializer())
    val vibes get() = related("vibes", Vibe.serializer())
}

@Serializable
data class MomentShareResult(val copies: Int? = null, val successMessage: String? = null)

@Serializable
data class NestedReference(
    override val id: String,
    val values: List<NestedValue>,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "NestedReference"
    companion object {
        val values = Column<NestedReference, List<NestedValue>>("values")
        fun find(id: String) = Query("NestedReference", serializer()).find(id)
        fun where(condition: Condition) = Query("NestedReference", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("NestedReference", serializer(), ids)
        fun entity(id: String) = entityOf("NestedReference", serializer(), id)
        suspend fun create(values: List<NestedValue>? = null): NestedReference {
            val values_ = buildJsonObject {
                values?.let { put("values", jsonValue(it)) }
            }
            return dbJson.decodeFromString(serializer(), DBlayer.database.create("NestedReference", values_.toString()))
        }
    }
    suspend fun update(values: List<NestedValue>? = null): NestedReference {
        val values_ = buildJsonObject {
            values?.let { put("values", jsonValue(it)) }
        }
        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values_.toString()))
    }
}

@Serializable
data class NestedSample(
    override val id: String,
    val value: NestedValue? = null,
    val values: List<NestedValue>,
    val tree: NestedTree? = null,
    val replyTo: MessageReplyNode? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "NestedSample"
    companion object {
        val value = Column<NestedSample, NestedValue?>("value")
        val values = Column<NestedSample, List<NestedValue>>("values")
        val tree = Column<NestedSample, NestedTree?>("tree")
        val replyTo = Column<NestedSample, MessageReplyNode?>("replyTo")
        fun find(id: String) = Query("NestedSample", serializer()).find(id)
        fun where(condition: Condition) = Query("NestedSample", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("NestedSample", serializer(), ids)
        fun entity(id: String) = entityOf("NestedSample", serializer(), id)
        suspend fun create(value: NestedValue? = null, values: List<NestedValue>? = null, tree: NestedTree? = null, replyTo: MessageReplyNode? = null): NestedSample {
            val values_ = buildJsonObject {
                value?.let { put("value", jsonValue(it)) }
                values?.let { put("values", jsonValue(it)) }
                tree?.let { put("tree", jsonValue(it)) }
                replyTo?.let { put("replyTo", jsonValue(it)) }
            }
            return dbJson.decodeFromString(serializer(), DBlayer.database.create("NestedSample", values_.toString()))
        }
    }
    suspend fun update(value: NestedValue? = null, values: List<NestedValue>? = null, tree: NestedTree? = null, replyTo: MessageReplyNode? = null): NestedSample {
        val values_ = buildJsonObject {
            value?.let { put("value", jsonValue(it)) }
            values?.let { put("values", jsonValue(it)) }
            tree?.let { put("tree", jsonValue(it)) }
            replyTo?.let { put("replyTo", jsonValue(it)) }
        }
        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values_.toString()))
    }
}

@Serializable
data class Presence(
    override val id: String,
    val userId: String,
    val room: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Presence"
    companion object {
        val userId = Column<Presence, String>("userId")
        val room = Column<Presence, String>("room")
        fun find(id: String) = Query("Presence", serializer()).find(id)
        fun where(condition: Condition) = Query("Presence", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("Presence", serializer(), ids)
        fun entity(id: String) = entityOf("Presence", serializer(), id)
        fun forUser(userId: String): Query<Presence> {
            var built = Query("Presence", serializer(), relation = "for_user", params = buildJsonObject { put("userId", jsonValue(userId)) })
            built = built.where(Condition.equal("userId", jsonValue(userId)))
            return built
        }
        val online: Query<Presence> get() {
            var built = Query("Presence", serializer(), relation = "online", params = buildJsonObject {  })
            return built
        }
        suspend fun create(userId: String, room: String): Presence {
            val values = buildJsonObject {
                put("userId", jsonValue(userId))
                put("room", jsonValue(room))
            }
            return dbJson.decodeFromString(serializer(), DBlayer.database.create("Presence", values.toString()))
        }
    }
    suspend fun update(userId: String? = null, room: String? = null): Presence {
        val values = buildJsonObject {
            userId?.let { put("userId", jsonValue(it)) }
            room?.let { put("room", jsonValue(it)) }
        }
        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values.toString()))
    }
    val user get() = related("user", User.serializer())
}

@Serializable
data class StorageRow(
    override val id: String,
    val record: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "StorageRow"
    companion object {
        val record = Column<StorageRow, String>("record")
        fun find(id: String) = Query("StorageRow", serializer()).find(id)
        fun where(condition: Condition) = Query("StorageRow", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("StorageRow", serializer(), ids)
        fun entity(id: String) = entityOf("StorageRow", serializer(), id)
        suspend fun create(record: String): StorageRow {
            val values = buildJsonObject {
                put("record", jsonValue(record))
            }
            return dbJson.decodeFromString(serializer(), DBlayer.database.create("StorageRow", values.toString()))
        }
    }
    suspend fun update(record: String? = null): StorageRow {
        val values = buildJsonObject {
            record?.let { put("record", jsonValue(it)) }
        }
        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values.toString()))
    }
}

@Serializable
data class User(
    override val id: String,
    val name: String,
    val avatar: JsonElement? = null,
    val birthday: String? = null,
    val typing: Boolean,
    val mood: String? = null,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "User"
    companion object {
        val name = Column<User, String>("name")
        val avatar = Column<User, JsonElement?>("avatar")
        val birthday = Column<User, String?>("birthday")
        fun find(id: String) = Query("User", serializer()).find(id)
        fun where(condition: Condition) = Query("User", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("User", serializer(), ids)
        fun entity(id: String) = entityOf("User", serializer(), id)
        val profile: Query<User> get() {
            var built = Query("User", serializer(), relation = "profile", params = buildJsonObject {  })
            return built
        }
        fun entity() = singletonEntityOf("User", serializer(), profile)
        suspend fun create(name: String, avatar: JsonElement? = null, birthday: String? = null): User {
            val values = buildJsonObject {
                put("name", jsonValue(name))
                avatar?.let { put("avatar", jsonValue(it)) }
                birthday?.let { put("birthday", jsonValue(it)) }
            }
            return dbJson.decodeFromString(serializer(), DBlayer.database.create("User", values.toString()))
        }
    }
    suspend fun update(name: String? = null, avatar: JsonElement? = null, birthday: String? = null): User {
        val values = buildJsonObject {
            name?.let { put("name", jsonValue(it)) }
            avatar?.let { put("avatar", jsonValue(it)) }
            birthday?.let { put("birthday", jsonValue(it)) }
        }
        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values.toString()))
    }
    suspend fun set(typing: Boolean? = null, mood: String? = null) {
        DBlayer.database.setEphemeral(modelName, id, buildJsonObject {
            typing?.let { put("typing", jsonValue(it)) }
            mood?.let { put("mood", jsonValue(it)) }
        }.toString())
    }
}

@Serializable
data class Vibe(
    override val id: String,
    val momentId: String,
    val userId: String,
    val kind: String,
    override val sync: SyncInfo = SyncInfo(),
    override val upload: UploadInfo? = null
) : DBModel {
    override val modelName get() = "Vibe"
    companion object {
        val momentId = Column<Vibe, String>("momentId")
        val userId = Column<Vibe, String>("userId")
        val kind = Column<Vibe, String>("kind")
        fun find(id: String) = Query("Vibe", serializer()).find(id)
        fun where(condition: Condition) = Query("Vibe", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("Vibe", serializer(), ids)
        fun entity(id: String) = entityOf("Vibe", serializer(), id)
        suspend fun create(momentId: String, userId: String, kind: String): Vibe {
            val values = buildJsonObject {
                put("momentId", jsonValue(momentId))
                put("userId", jsonValue(userId))
                put("kind", jsonValue(kind))
            }
            return dbJson.decodeFromString(serializer(), DBlayer.database.create("Vibe", values.toString()))
        }
    }
    suspend fun update(momentId: String? = null, userId: String? = null, kind: String? = null): Vibe {
        val values = buildJsonObject {
            momentId?.let { put("momentId", jsonValue(it)) }
            userId?.let { put("userId", jsonValue(it)) }
            kind?.let { put("kind", jsonValue(it)) }
        }
        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values.toString()))
    }
}

@Serializable
data class NestedValue(
    val title: String,
    val note: String? = null,
    val nullableDefault: String? = "default",
    @SerialName("owner_id") @Serializable(with = IDText::class) val ownerId: String? = null,
    val count: Long = 2L,
    val ratio: Double = 1.5,
    val enabled: Boolean = false,
    val createdAt: String? = null,
    val state: String = "ready",
    val labels: List<String> = listOf(),
    val counts: List<Long> = listOf(),
    val ratios: List<Double> = listOf(),
    val flags: List<Boolean> = listOf(),
    val dates: List<String> = listOf(),
    @Serializable(with = IDTexts::class) val owners: List<String> = listOf(),
    val payload: JsonElement? = null,
    val rawValue: JsonElement = JsonObject(mapOf("free" to JsonArray(listOf(JsonPrimitive(1), JsonPrimitive(true), JsonNull)))),
    val preferences: NestedValuePreferences = NestedValuePreferences(label = "base"),
    val presets: List<NestedValuePreferences> = listOf(NestedValuePreferences(label = "first")),
    val metadata: JsonElement? = null,
    val values: JsonElement? = null,
    val child: NestedValueChild? = null,
    val children: List<NestedValueChild> = listOf(),
)

@Serializable
data class NestedValuePreferences(
    val label: String,
    val active: Boolean = true,
)

@Serializable
data class NestedValueChild(
    val body: String? = null,
    @SerialName("x") val horizontal: Double = 0.0,
)

@Serializable
data class NestedTree(
    val name: String,
    val children: List<NestedTree> = listOf(),
)

@Serializable
data class MessageReplyNode(
    @Serializable(with = IDText::class) val id: String? = null,
    val body: String? = null,
    val kind: String? = null,
    @Serializable(with = IDText::class) val userId: String? = null,
    val user: MessageUserRef? = null,
    val media: NestedMedia? = null,
)

@Serializable
data class MessageUserRef(
    @Serializable(with = IDText::class) val id: String? = null,
    val fullName: String? = null,
    val name: String? = null,
    val username: String? = null,
    val avatarUrl: String? = null,
)

@Serializable
data class NestedMedia(
    @Serializable(with = IDText::class) val id: String = "",
    val fileUrl: String? = null,
)

val modelsJSON = """[{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"messages","kind":"has_many","target":"Message","foreign_key":"chatId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastMessage","kind":"belongs_to","target":"Message","foreign_key":"lastMessageId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastMessageAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"composite_indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateChat","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"chat","kind":"belongs_to","target":"Chat","foreign_key":"chatId","dependent":null,"touches_parent":false,"sets":{"lastMessageAt":"createdAt","lastMessageId":"id"},"newer":[{"child":"createdAt","parent":"lastMessageAt"}],"from":null}],"relations":[{"name":"by_chat","by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false,"joins_created":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ChatMessages","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"composite_indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMessage","variables":{"chatId":"chatId","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":["authorId"],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"object"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"feed","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"composite_indexes":[["userId","createdAt"]],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMoment","variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"UpdateMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"DeleteMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"LikeMoment","variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"ShareMoment","variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"NestedReference","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null}],"associations":[],"relations":[],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"NestedSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null,"fields":[{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"note","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"nullableDefault","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":"default","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ownerId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"owner_id","app_type":null,"merge":null},{"name":"count","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":2,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratio","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":1.5,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"enabled","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"enum","values":["ready","done"]},"nullable":false,"optional":false,"default":"ready","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"labels","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"counts","kind":{"type":"list","element":"int"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratios","kind":{"type":"list","element":"num"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"flags","kind":{"type":"list","element":"bool"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"dates","kind":{"type":"list","element":"date"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"owners","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"payload","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"rawValue","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{"free":[1,true,null]},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"preferences","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"label":"base"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null,"fields":[{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":true,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"presets","kind":{"type":"array"},"nullable":false,"optional":false,"default":[{"label":"first"}],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null},{"name":"metadata","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"horizontal","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":"x","app_type":null,"merge":null}]},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null}]},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null},{"name":"tree","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null,"fields":[{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null}]},{"name":"replyTo","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageReplyNode","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageUserRef","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fullName","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"username","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatarUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedMedia","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":"","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"online","by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"StorageRow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"record","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"composite_indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"""
