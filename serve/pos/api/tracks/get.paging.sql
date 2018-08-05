--($parent.page integer)--

SELECT 
    (CASE 
        WHEN total_pages = CAST(total_pages AS INTEGER) THEN 
            CAST(total_pages AS INTEGER)
        ELSE
           1 + CAST(total_pages AS INTEGER)
    END) as total_pages, 
    {{$parent.page}} as current_page
FROM 
    (SELECT (count(*) / 10.00) as total_pages FROM 
    playlist_track pt
JOIN 
    playlists p ON p.playlistid = pt.playlistid
JOIN
    tracks t ON t.trackid = pt.trackid
ORDER BY
    t.trackid) t
