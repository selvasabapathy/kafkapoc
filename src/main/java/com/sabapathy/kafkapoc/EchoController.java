package com.sabapathy.kafkapoc;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = { "/kafka/{msg}" })
public class EchoController
{
    @GetMapping
    public ResponseEntity refreshConsent(@PathVariable String msg)
    {
        System.out.println("Msg: {}" + msg);
        return new ResponseEntity(HttpStatus.OK);
    }
}
