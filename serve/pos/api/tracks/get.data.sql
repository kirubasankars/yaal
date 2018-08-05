--($parent.page integer, $parent.playlistid integer)--

SELECT
    t.*, p.Name as PlayListName
FROM 
    playlist_track pt
JOIN 
    playlists p on p.playlistid = pt.playlistid
JOIN 
    tracks t on pt.trackid = t.trackid
WHERE 
    pt.trackid= 3500 
    
