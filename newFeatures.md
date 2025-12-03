TODO list
-------


- the app refreshes on interval. Why is it doing like that? wouldnt it be better with a websocket?

- Implement a secondary spotify sign in. so, in main settings, basically add another "spotify setup". maybe call them Account 1 and 2. And in the main screen i should be able to specify if a node should get music from channel 1 or channel 2 (spotify account 1 or 2). Basically add a toggle button that toggles between CH1 and CH2. Switching between the two accounts in the player-panel chouls be accomplished by swiping the whole player-panel ro the side. So one large swipe should switch to the next player for the next account. This app must still be usable with only one spotify account


- If im playing from a playlist, and select the playlist button, it opens th emodal where i can see all songs in that playlist, but i also see that its actually loading all the playlists. This is not nessessary. So instead of loading in the playlists, just load in the songs, and when i press the "all playlists" bustton (the back button), then you can load the playlists. 

- i want to be able to turn the computer running the RoomCast server into a client aswell. Preferably, i should be able to press the "Add node" button at top and have a option to "Add server as node" or something like that. It shis achieveable, or do we have to then have a different docker compose that also includes the snapserver image?

- When i open the individual node settings, i want a button that lets me access the node terminal. Can we achieve that?

- If i open any settings menu when the "take over session" popup is visible, then the popup covers the menus also. It should not be placed above the menus or other popup modals.

