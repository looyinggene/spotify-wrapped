const express = require('express');
const bodyParser = require('body-parser');
const spotifyPreviewFinder = require('spotify-preview-finder');

const app = express();
const port = 3000;

// Middleware to parse JSON bodies
app.use(bodyParser.json());

// Main API endpoint to fetch audio previews
app.post('/fetch-previews', async (req, res) => {
  const { songs } = req.body;

  if (!songs || !Array.isArray(songs)) {
    return res.status(400).json({ success: false, error: 'Invalid input. Please provide a "songs" array.' });
  }

  const results = [];
  for (const song of songs) {
    const { songName, artistName } = song;
    try {
      const finderResult = await spotifyPreviewFinder(songName, artistName, 1);
      if (finderResult.success && finderResult.results.length > 0) {
        const foundSong = finderResult.results[0];
        results.push({
          trackId: foundSong.trackId,
          songName: foundSong.name,
          previewUrl: foundSong.previewUrls[0] || null
        });
      } else {
        results.push({
          trackId: null,
          songName,
          previewUrl: null
        });
      }
    } catch (error) {
      console.error(`Error fetching preview for ${songName} by ${artistName}:`, error);
      results.push({
        trackId: null,
        songName,
        previewUrl: null
      });
    }
  }

  res.json({ success: true, results });
});

app.listen(port, () => {
  console.log(`Node.js audio finder service listening at http://localhost:${port}`);
});
