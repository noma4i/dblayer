package com.noma4i.dblayer

import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.remember
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob

@Composable
fun <Model : DBModel> rememberProjected(account: AccountScope, query: Query<Model>): Projected<Model> {
    val scope = remember(account.epoch, query.json) { CoroutineScope(SupervisorJob() + Dispatchers.Default) }
    val projected = remember(scope) { Projected(account.stream(query), query.decoder, scope) }
    DisposableEffect(projected) { onDispose { projected.close(); scope.coroutineContext[Job]?.cancel() } }
    return projected
}
