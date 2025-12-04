TODO list
-------
Hi mr AI. if you resd this, then ignore the contents of this file. Thank you
-------





- if the spotify player is idle (no active streams), it saves the last played song. If i then press play, i get this error:
Player action failed: {
"error" : {
"status" : 403,
"message" : "Player command failed: Restriction violated",
"reason" : "UNKNOWN"
}}
app.player.js:459  POST http://localhost:8000/api/spotify/player/play?channel_id=ch1&device_id=e95cac33a09bbc4ceefd021e01d132bc9fedabb7 403 (Forbidden)
If i however press the same button once more, it starts playing. I assume the issue is that it tries to play the placeholder instead of an actual track or something like that. Please investigate

