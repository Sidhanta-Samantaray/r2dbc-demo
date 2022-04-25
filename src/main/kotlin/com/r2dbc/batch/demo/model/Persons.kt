package com.r2dbc.batch.demo.model

import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import org.springframework.stereotype.Component
import java.util.function.BiFunction

data class Person(val id: Int, val firstName: String, val lastName: String, val age: Int)


@Component
class PersonsMapper : BiFunction<Row, RowMetadata, Person> {
    override fun apply(t: Row, u: RowMetadata): Person {
        return Person(
            id = t["id"] as Int,
            firstName = t["first_name"] as String,
            lastName = t["last_name"] as String,
            age = t["age"] as Int
        )
    }
}
