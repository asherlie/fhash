import spotipy
import json
from spotipy.oauth2 import SpotifyClientCredentials

def getspot():
    spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())
    return spotify

spotify = getspot()

# wow, both audio features and audio analysis are really interesting
# analysis contains start time and duration of every beat!
# as well as a section analysis - keys, time sigs, tempos of different sections
def new_releases(n):
    ret = []
    target = n//50
    plen = -1
    # for i in range(target+1):
    while len(ret) < n:
        progress = len(ret)
        if progress == plen:
            break
        plen = progress
        tmp = spotify.new_releases(limit=min(50, n-progress), offset=progress)
        for album in tmp['albums']['items']:
            # ret.append(album['name'])
            tracks = spotify.album_tracks(album['id'])['items']
            for track in tracks:
                features = spotify.audio_features(track['id'])
                obj = (track['name'], track['artists'][0]['name'], features[0])
                ret.append(obj)
                # ret.append(track['name'])
        # ret.append(tmp['albums']['items'])
    return ret

def load_releases(file):
    with open(file, 'r') as f:
        return json.load(f)

# print(new_releases(4))
