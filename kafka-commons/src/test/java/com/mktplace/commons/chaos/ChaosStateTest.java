package com.mktplace.commons.chaos;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ChaosStateTest {

    @Test
    void failNextConsomeUmaFalhaPorTentativa() {
        ChaosState state = new ChaosState();
        state.failNext(2, ChaosMode.POISON);
        assertThat(state.takeFailure()).isEqualTo(ChaosMode.POISON);
        assertThat(state.takeFailure()).isEqualTo(ChaosMode.POISON);
        assertThat(state.takeFailure()).isNull();
        assertThat(state.failRemaining()).isZero();
    }

    @Test
    void slowConsomeContagemEDevolveLatencia() {
        ChaosState state = new ChaosState();
        state.slow(300, 1);
        assertThat(state.takeSlow()).isEqualTo(300);
        assertThat(state.takeSlow()).isZero();
    }

    @Test
    void clearZeraTudo() {
        ChaosState state = new ChaosState();
        state.failNext(5, ChaosMode.TRANSIENT);
        state.slow(100, 5);
        state.clear();
        assertThat(state.takeFailure()).isNull();
        assertThat(state.takeSlow()).isZero();
    }
}
