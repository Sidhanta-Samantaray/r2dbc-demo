package com.r2dbc.batch.demo.controller

import com.r2dbc.batch.demo.model.Person
import com.r2dbc.batch.demo.repo.PersonRepoV2
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v2/person")
class PersonControllerV2(private val personRepo: PersonRepoV2) {

    @GetMapping
    suspend fun findAll(): Flow<Person> = personRepo.findAll()

    @PostMapping
    suspend fun save(@RequestBody person: Person) = personRepo.save(person)

    @FlowPreview
    @PostMapping("/bulk")
    suspend fun saveAll(
        @RequestBody personList: List<Person>,
        @RequestParam(required = false) batchMode: Boolean
    ) = if (batchMode) personRepo.saveBulk(personList) else personRepo.saveAll(personList)

    /*@FlowPreview
    @PostMapping("/batch")
    suspend fun saveBatch(
        @RequestBody personList: List<Person>
    ) = personRepo.save(personList)*/
}
