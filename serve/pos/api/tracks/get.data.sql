--($parent.page integer)--

SELECT 
    p.Playlistid, p.Name as PlayListName,t.TrackId, t.Name as TrackName
FROM 
    playlist_track pt
JOIN 
    playlists p ON p.playlistid = pt.playlistid
JOIN
    tracks t ON t.trackid = pt.trackid
ORDER BY
    t.trackid
LIMIT ({{$parent.page}} - 1) * 10, 10

    
