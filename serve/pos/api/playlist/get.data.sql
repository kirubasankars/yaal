SELECT 
    p.Playlistid, p.Name as PlayListName, count(*) as TracksCount
FROM
    playlist_track pt
JOIN 
    playlists p ON p.playlistid = pt.playlistid
JOIN
    tracks t ON t.trackid = pt.trackid
GROUP BY 
    p.playlistid, p.Name
