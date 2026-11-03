package com.noma4i.dblayer

import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.State
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.remember
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel

@Composable
fun <Value> LiveSource<Value>.collectAsLive(): State<Value> = flow.collectAsState(initial)

@Composable
fun <Model : DBModel> Query<Model>.collectAsLive(): State<List<Model>> = live.collectAsLive()

@Composable
fun <Model : DBModel> rememberLiveList(query: Query<Model>, refreshOnConnect: Boolean = false): LiveList<Model> {
    val scope = rememberLiveScope(query.json, refreshOnConnect)
    return remember(scope) { LiveList(query, scope, refreshOnConnect) }
}

@Composable
fun <Model : DBModel> rememberLiveList(related: RelatedQuery<Model>, refreshOnConnect: Boolean = false): LiveList<Model> {
    val scope = rememberLiveScope(related.name, related.sourceJsonText(refreshOnConnect))
    return remember(scope) { LiveList(related, scope, refreshOnConnect) }
}

@Composable
fun <Model : DBModel> RelatedQuery<Model>.collectAsLive(): State<List<Model>> = live.collectAsLive()

@Composable
private fun rememberLiveScope(vararg keys: Any?): CoroutineScope {
    val scope = remember(*keys) { CoroutineScope(SupervisorJob()) }
    DisposableEffect(scope) { onDispose { scope.cancel() } }
    return scope
}
