# RoomCast Mini Wiring Guide (ESP32-S3 + PCM5102A + TPA3118, Mono)

Use these firmware pin mappings (from `nodes/esp32/main/roomcast_config.h`):
- `BCK = GPIO17`
- `LRCK/WS = GPIO18`
- `DATA = GPIO16`

## 1. Power Topology
1. Set the USB-C PD trigger board to `15V`.
2. Connect PD `V+` to `TPA3118 VIN`.
3. Connect PD `V+` to `LM2596 IN+`.
4. Connect PD `GND` to `TPA3118 GND`.
5. Connect PD `GND` to `LM2596 IN-`.
6. Adjust LM2596 output to `5.0V-5.1V` before connecting ESP32 and DAC.
7. Connect LM2596 `OUT+` to ESP32 `5V/VBUS` input pin.
8. Connect LM2596 `OUT-` to ESP32 `GND`.
9. Power DAC from the logic rail:
   - If DAC supports `5V` input, connect DAC `VCC` to LM2596 `OUT+`.
   - If DAC is `3.3V-only`, connect DAC `VCC` to ESP32 `3V3`.
10. Connect DAC `GND` to common `GND`.

## 2. ESP32 to DAC (I2S)
1. ESP32 `GPIO17` -> DAC `BCK` (or `SCK/BCLK`).
2. ESP32 `GPIO18` -> DAC `LRCK` (or `WS/LCK`).
3. ESP32 `GPIO16` -> DAC `DIN` (or `DATA`).
4. ESP32 `GND` -> DAC `GND`.
5. Leave `MCLK` unconnected.

## 3. DAC to Amplifier (Mono Input)
1. Quick method: DAC `L_OUT` -> amp signal input, DAC `AGND/GND` -> amp signal ground.
2. Better mono method: resistor-sum `L_OUT` and `R_OUT` with `1k + 1k` into one mono node, then mono node -> amp input, DAC ground -> amp signal ground.
3. Do not short `L_OUT` and `R_OUT` directly together.

## 4. Amplifier to Speaker
1. Amp `OUT+` -> speaker `+`.
2. Amp `OUT-` -> speaker `-`.
3. Keep polarity consistent.
4. Do not connect amp output terminals to ground (BTL output stage).

## 5. Grounding and Noise Rules
1. Use one shared ground reference for PD, buck, ESP32, DAC, and amp.
2. Keep amp power wiring separate from DAC analog lines.
3. Keep DAC analog wires short.
4. Use thicker wires for amp power and speaker output.

## 6. First Power-Up Sequence
1. Power PD and buck first, then confirm buck output is `5.0V-5.1V`.
2. Add ESP32 and verify boot and Wi-Fi.
3. Add DAC I2S wires.
4. Add amp input and speaker last.
5. Start playback at low volume first.

## 7. Bring-Up Test Checklist
1. Confirm RoomCast node appears in the app.
2. Start playback and verify clean audio.
3. Test stop/play repeatedly and listen for pops or dropouts.
4. Increase volume gradually and check amp temperature.
5. If hum or noise appears, re-check ground routing and analog cable placement.
