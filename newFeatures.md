TODO list
-------



- If im playing from a playlist, and select the playlist button, it opens the modal where i can see all songs in that playlist, but i also see that its actually loading all the playlists. This is not nessessary. So instead of loading in the playlists, just load in the songs, and when i press the "all playlists" bustton (the back button), then you can load the playlists. 

- i want to be able to turn the computer running the RoomCast server into a client aswell, this way i can install everything in a raspberry pi and that raspberry py will be the main controller AND one of my clients. Preferably, i should be able to press the "Add node" button at top and have a option to "Add server as node" or something like that. Is this achieveable, or do we have to then have a different docker compose that also includes the snapserver image?

- It should be possible to assign color themes for the different channels. As dar as i can see, these are the used colors:
    background: #0b1020;
    color: #e2e8f0;
    --accent: #22c55e;
    --accent-dark: #16a34a;
    --panel: rgba(15, 23, 42, 0.92);
And it looks to me that its mainly the greens that should be channel spesific

- Remove "user-status" from main screen and move it to the top of main settings