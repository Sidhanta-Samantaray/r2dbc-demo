package com.r2dbc.batch.demo.repo

import com.r2dbc.batch.demo.model.Person
import com.r2dbc.batch.demo.model.PersonsMapper
import io.r2dbc.spi.Connection
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitOne
import org.springframework.r2dbc.core.flow
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux

@Repository
class PersonRepoV2(private val databaseClient: DatabaseClient, private val personsMapper: PersonsMapper) {
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

    suspend fun findAll(): Flow<Person> {
        return databaseClient.sql("""select * from persons""")
            .map(personsMapper::apply)
            .flow()
    }

    suspend fun save(person: Person): Person {
        return databaseClient.sql(insertSql)
            .filter { statement, _ -> statement.returnGeneratedValues(*returnCols).execute() }
            .bind("firstName", person.firstName)
            .bind("lastName", person.lastName)
            .bind("age", person.age)
            .fetch()
            .awaitOne().run {
                mapToPerson(this)
            }
    }

    suspend fun saveAll(personList: List<Person>): Flow<Person> {
        return personList.map { person: Person -> save(person) }
            .asFlow()
    }

   /* @FlowPreview
    suspend fun saveBulk(personList: List<Person>): Flow<Person> {
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
        }.asFlow()
    }*/
    @FlowPreview
    suspend fun saveBulk(personList: List<Person>): Flow<Person> {
        return databaseClient.connectionFactory.create().asFlow()
            .flatMapMerge { value: Connection ->
                value.createStatement(insertSql2)
                    .returnGeneratedValues(*returnCols)
                    .run {
                        for (p in personList) {
                            this.bind(0, p.firstName)
                                .bind(1, p.lastName)
                                .bind(2, p.age)
                                .add()
                        }
                        execute()
                    }.asFlow()
                    .map { item -> item.map { t, u -> personsMapper.apply(t, u) }.awaitSingle() }
            }
    }
}
