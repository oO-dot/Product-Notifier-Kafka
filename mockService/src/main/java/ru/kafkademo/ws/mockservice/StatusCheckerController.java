package ru.kafkademo.ws.mockservice;


import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/response")
public class StatusCheckerController {

    @GetMapping("/200")
    ResponseEntity<String> response200String() {
        return ResponseEntity.ok("OK");
    }

    @GetMapping("/500")
    ResponseEntity<String> response500String() {
        return ResponseEntity.internalServerError().build();
    }
}
