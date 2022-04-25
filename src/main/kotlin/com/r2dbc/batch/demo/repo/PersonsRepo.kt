package com.r2dbc.batch.demo.repo

import com.r2dbc.batch.demo.model.Person
import com.r2dbc.batch.demo.model.PersonsMapper
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Repository
class PersonsRepo(val databaseClient: DatabaseClient, val personsMapper: PersonsMapper) {
    val insertSql = "INSERT INTO persons(first_name, last_name, age) values(:firstName,:lastName,:age)"
    val insertSql2 = "INSERT INTO persons(first_name, last_name, age) values($1,$2,$3)"
    val returnCols = arrayOf("id", "first_name", "last_name", "age")
    fun mapToPerson(row: Map<String, Any>) =
        Person(
            row["id"] as Int,
            row["first_name"] as String,
            row["last_name"] as String,
            row["age"] as Int
        )

    fun findAll(): Flux<Person> {
        return databaseClient.sql("select * from persons")
            .map(personsMapper::apply)
            .all()
    }
    fun findById(id: Int): Mono<Person> {
        return databaseClient.sql("select * from persons where id=:id")
            .bind("id", id)
            .map(personsMapper::apply)
            .one()
    }
    fun saveOne(person: Person): Mono<Person> {

        return databaseClient.sql(insertSql)
            .filter { statement, _ -> statement.returnGeneratedValues("id", "first_name", "last_name", "age").execute() }
            .bind("firstName", person.firstName)
            .bind("lastName", person.lastName)
            .bind("age", person.age)
            .fetch()
            .one()
            .map(this::mapToPerson)
    }
    fun saveAllOld(personList: List<Person>): Mono<Void> {
        personList.map {
            saveOne(it).doOnNext {
                    it ->
                it
            }.subscribe()
        }
        return Mono.empty()
    }
    fun saveAll(personList: List<Person>): Flux<Person> {
        return databaseClient.inConnectionMany {
                connection ->
            val insertStmt = connection.createStatement(insertSql2)
                .returnGeneratedValues(*returnCols)
            for (p in personList) {
                insertStmt.bind(0, p.firstName)
                    .bind(1, p.lastName)
                    .bind(2, p.age)
                    .add()
            }
            Flux.from(insertStmt.execute())
                .flatMap { result -> result.map(personsMapper::apply) }
        }
    }
    fun saveUsingBatch(personList: List<Person>): Flux<Person> {
        val flux = databaseClient.inConnectionMany { connection ->
            val batch = connection.createBatch()
            personList.forEach { person: Person ->
                batch.add(
                    """INSERT INTO persons(first_name, last_name, age) values('${person.firstName}'
                            |,'${person.lastName}','${person.age}') 
                    """.trimMargin()
                )
            }
            Flux.from(batch.execute())
        }
        return flux.flatMap { result ->
            result.map { t, u -> personsMapper.apply(t, u) }
        }
    }
    fun saveBatch2(): Flux<Person> {
        return Mono.from(
            databaseClient.inConnectionMany {
                    connection ->
                Flux.from(
                    connection.createBatch()
                        .add("INSERT INTO persons(first_name, last_name, age) VALUES('tim','jones','22')")
                        .add("INSERT INTO persons(first_name, last_name, age) VALUES('Mark','Whal','49')")
                        .execute()
                )
            }
        ).flux()
            .flatMap { result ->
                result.map { t, u ->
                    println("Test :-"+t[0] as String)
                    personsMapper.apply(t, u)
                }
            }
    }
}
