package com.noma4i.dblayer

import androidx.compose.runtime.Composable
import androidx.compose.runtime.State
import androidx.compose.runtime.collectAsState

@Composable
fun <Value> LiveSource<Value>.collectAsLive(): State<Value> = flow.collectAsState(initial)

@Composable
fun <Model : DBModel> Query<Model>.collectAsLive(): State<List<Model>> = live.collectAsLive()
