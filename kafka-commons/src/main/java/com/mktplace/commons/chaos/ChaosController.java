package com.mktplace.commons.chaos;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Rotas de chaos (protegidas por X-Admin-Token via AdminTokenFilter). */
@RestController
@RequestMapping("/admin/chaos")
public class ChaosController {

    private final ChaosService chaos;

    public ChaosController(ChaosService chaos) {
        this.chaos = chaos;
    }

    @PostMapping("/fail-next")
    public ChaosStatus failNext(@RequestParam int count,
                                @RequestParam(defaultValue = "TRANSIENT") ChaosMode exception) {
        return chaos.failNext(count, exception);
    }

    @PostMapping("/slow")
    public ChaosStatus slow(@RequestParam long ms, @RequestParam(defaultValue = "1000000") int count) {
        return chaos.slow(ms, count);
    }

    @PostMapping("/pause")
    public ChaosStatus pause() {
        return chaos.pause();
    }

    @PostMapping("/resume")
    public ChaosStatus resume() {
        return chaos.resume();
    }

    @PostMapping("/crash")
    public ResponseEntity<Void> crash() {
        chaos.crash();
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }

    @GetMapping("/status")
    public ChaosStatus status() {
        return chaos.status();
    }

    @DeleteMapping
    public ChaosStatus clear() {
        return chaos.clear();
    }
}
