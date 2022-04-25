package com.r2dbc.batch.demo.controller

import com.r2dbc.batch.demo.model.Person
import com.r2dbc.batch.demo.repo.PersonsRepo
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/person")
class PersonController(val personsRepo: PersonsRepo) {

    @GetMapping("/all")
    @ResponseBody fun findAll(): Flux<Person> {
        return personsRepo.findAll()
    }
    @GetMapping("/{id}")
    @ResponseBody fun findById(@PathVariable id: Int): Mono<Person> {
        return personsRepo.findById(id)
    }

    @PostMapping
    @ResponseBody fun savePerson(@RequestBody person: Person): Mono<Person> {
        return personsRepo.saveOne(person)
    }
    @PostMapping("/all")
    @ResponseBody fun saveAll(@RequestBody personList: List<Person>): Mono<Void> {
        return personsRepo.saveAllOld(personList)
    }
    @PostMapping("/bulk")
    @ResponseBody fun saveBulk(@RequestBody personList: List<Person>): Flux<Person> {
        return personsRepo.saveAll(personList)
    }
    @PostMapping("/batch")
    @ResponseBody fun saveBatch(@RequestBody personList: List<Person>): Flux<Person> {
        return personsRepo.saveBatch2()
    }
}
