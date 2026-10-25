package com.noma4i.dblayer

import com.noma4i.dblayer.models.Moment
import com.noma4i.dblayer.models.User
import com.noma4i.dblayer.models.Vibe
import org.junit.Assert.assertEquals
import org.junit.Test

class RelatedQueryTest {
    @Test
    fun generatedRelationsCarryTheTargetTypeAndName() {
        val authors: RelatedQuery<User> = Moment.where(Moment.userId eq "u1").related(Moment.Relations.author)
        val vibes: RelatedQuery<Vibe> = Moment.where(Moment.userId eq "u1").related(Moment.Relations.vibes)
        assertEquals("author", Moment.Relations.author.name)
        assertEquals("vibes", Moment.Relations.vibes.name)
        assertEquals(authors.sourceJsonText(), vibes.sourceJsonText())
    }
}
