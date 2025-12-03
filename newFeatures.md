TODO list
-------
Hi mr AI. if you resd this, then ignore the contents of this file. Thank you
-------

- If im playing from a playlist, and select the playlist button, it opens the modal where i can see all songs in that playlist, but i also see that its actually loading all the playlists. This is not nessessary. So instead of loading in the playlists, just load in the songs, and when i press the "all playlists" bustton (the back button), then you can load the playlists. 

- i want to be able to turn the computer running the RoomCast server into a client aswell, this way i can install everything in a raspberry pi and that raspberry py will be the main controller AND one of my clients. Preferably, i should be able to press the "Add node" button at top and have a option to "Add server as node" or something like that. Is this achieveable, or do we have to then have a different docker compose that also includes the snapserver image?

- when creating creadentials for spotify, i assume the first thing you must do is open spotfy developer dashboard, Therefore, move that button to the top of the spotify setup form.

- In channels editor thingy, remove channel-color-swatch. Its not needed as channel-color-inputs already have a color selector

- remove the "NODES" section text in main screen


- In the node list, replace the "Online" and "Offline" with a wifi icon indicating the wifi strength of the node. Here you must get wifi signal from the node and you must switch between wifi icons fo signal the different strengths. Also, if the node is disconnected, the icon should reflect that aswell.