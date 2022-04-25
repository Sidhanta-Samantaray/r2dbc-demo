package com.r2dbc.batch.demo.controller

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/ping")
class PingController {

    @GetMapping
    @ResponseBody fun ping(): Mono<Map<String, String>> {

        return Mono.just(mapOf("message" to "Application Up !!!"))
    }
}
