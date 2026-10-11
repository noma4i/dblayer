package com.noma4i.dblayer.models

import com.noma4i.dblayer.*
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
        val createdAt = Column<Moment, String>("createdAt")
        fun find(id: String) = Query("Moment", serializer()).find(id)
        fun where(condition: Condition) = Query("Moment", serializer()).where(condition)
        fun ids(ids: List<String>) = idsQuery("Moment", serializer(), ids)
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
        val profile: Query<User> get() {
            var built = Query("User", serializer(), relation = "profile", params = buildJsonObject {  })
            return built
        }
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

val modelsJSON = """[{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"messages","kind":"has_many","target":"Message","foreign_key":"chatId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastMessage","kind":"belongs_to","target":"Message","foreign_key":"lastMessageId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastMessageAt","direction":"desc"}]},"joins_events":false}],"composite_indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateChat","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"chat","kind":"belongs_to","target":"Chat","foreign_key":"chatId","dependent":null,"touches_parent":false,"sets":{"lastMessageAt":"createdAt","lastMessageId":"id"},"newer":[{"child":"createdAt","parent":"lastMessageAt"}],"from":null}],"relations":[{"name":"by_chat","by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ChatMessages","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false}],"composite_indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMessage","variables":{"chatId":"chatId","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":["authorId"],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"object"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false},{"name":"feed","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false}],"composite_indexes":[["userId","createdAt"]],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMoment","variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"UpdateMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"DeleteMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"LikeMoment","variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"ShareMoment","variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false},{"name":"online","by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false}],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false}],"composite_indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"""
