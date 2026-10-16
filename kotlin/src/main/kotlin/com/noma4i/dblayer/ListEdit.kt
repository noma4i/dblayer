package com.noma4i.dblayer

/** One step that turns the rows a reader holds into the next rows. Every step carries the value of its own moment, so steps from several deliveries replay in order without reading the final rows. */
sealed class ListEdit<out Model> {
    data class Rewrite<out Model>(val index: Int, val row: Model) : ListEdit<Model>()
    data class Insert<out Model>(val index: Int, val row: Model) : ListEdit<Model>()
    data class Remove(val index: Int, val id: String) : ListEdit<Nothing>()

    internal companion object {
        /** The steps of two deliveries in order, or null when either came without steps or replaying them costs more than taking the `rows` whole. */
        fun <Model> composing(older: List<ListEdit<Model>>?, newer: List<ListEdit<Model>>?, within: Int): List<ListEdit<Model>>? {
            if (older == null || newer == null || older.size + newer.size > within) return null
            return older + newer
        }
    }
}

/** Applies the steps in order. The steps come from the same library that delivered the rows, so an index outside the list is a defect there. */
fun <Model> MutableList<Model>.replay(edits: List<ListEdit<Model>>) {
    for (edit in edits) when (edit) {
        is ListEdit.Rewrite -> this[edit.index] = edit.row
        is ListEdit.Insert -> add(edit.index, edit.row)
        is ListEdit.Remove -> removeAt(edit.index)
    }
}
