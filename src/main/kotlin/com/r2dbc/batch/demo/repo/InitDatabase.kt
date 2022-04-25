package com.r2dbc.batch.demo.repo

import org.springframework.boot.CommandLineRunner
import org.springframework.core.annotation.Order
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
@Order(2)
class InitDatabase(val databaseClient: DatabaseClient) : CommandLineRunner {

    override fun run(vararg args: String) {
        println(databaseClient.toString())
        databaseClient.sql(
            """
   CREATE TABLE IF NOT EXISTS persons (
  id SERIAL PRIMARY KEY, 
  first_name VARCHAR(255), 
  last_name VARCHAR(255), 
  age INTEGER
)   """
        ).then()
            .block()
        databaseClient.sql(
            """
    INSERT INTO persons(first_name, last_name, age)
    VALUES
    ('Hello', 'Kitty', 20),
    ('Hantsy', 'Bai', 40)
    """
        ).then().block()

        val count: Mono<Long> = databaseClient.sql("select count(1) from persons")
            .map { row -> row[0] as Long }
            .one()
        count.subscribe { println("created $it records") }
    }
}
