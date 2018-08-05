--($parent.page integer, $parent.playlistid integer)--

SELECT 
    count(*) / 10 as total_pages
FROM 
    playlist_track pt
JOIN 
    playlists p ON p.playlistid = pt.playlistid
JOIN
    tracks t ON t.trackid = pt.trackid
JOIN
    genres g ON g.genreid = t.genreid
JOIN 
    albums a ON a.AlbumId = t.AlbumId
JOIN 
    media_types m ON m.mediatypeid = t.mediatypeid 
WHERE 
    {{$parent.playlistid}} is null or p.playlistid = {{$parent.playlistid}}
