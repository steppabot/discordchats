# Wolfpac Arcade

An additive arcade page for DiscordChats: your server chat on the left and a
game picker/player on the right. The existing archive remains at `/`.

## Included

- `arcade.html`: self-contained arcade frontend in the existing DiscordChats
  palette, with a link back to the archive.
- `games/helicopter.html`: standalone, embeddable helicopter cave game. Open
  this file directly, or host it with GitHub Pages. It needs no dependencies.
- `live_chat.py`: optional FastAPI routes under `/api/arcade`, using the
  site's existing Discord login, bot, guild, and booster/subscription gate.
- `arcade-src/`: editable frontend source and the build command.

The helicopter artwork is newly drawn in canvas, inspired by the supplied
reference. Hold Space, Up Arrow, mouse, or touch to rise; release to fall.
P pauses. Switching tabs or focusing the chat composer pauses the game.
Personal best is saved locally in the browser, not as a verified leaderboard.

## Deployment on the current website

1. Deploy the updated `app.py` and new `live_chat.py` to the existing Python
   API host. No new runtime dependencies or database migrations are needed.
2. On that host, set **`ARCADE_CHANNEL_IDS`** to a comma-separated list of up
   to 20 text channel IDs from your existing `DISCORD_GUILD_ID`.
3. Keep `BOT_TOKEN` (or `DISCORD_TOKEN`), Discord client credentials,
   `DISCORD_REDIRECT_URI`, and `SITE_BASE_URL` as currently configured. Set
   `SESSION_SECRET` to a securely generated secret of at least 32 characters
   if it does not already meet that requirement. Rotating it signs users out.
4. Give the existing bot **View Channel**, **Read Message History**, and
   **Send Messages** in each selected channel. Enable **Message Content
   Intent** for that Discord application. Without it, ordinary message
   content may be empty. Do not grant Administrator just for the arcade.
5. Publish the frontend through the existing GitHub Pages workflow. The
   archive menu links to `/arcade.html`, and the standalone game is at
   `/games/helicopter.html`.
6. Test with an allowed member and a member denied access to a channel. Check
   sign-in, message reads/sends, logout, desktop controls, and mobile touch.

Missing configuration leaves chat disconnected while games remain playable.
The OAuth callback URL stays the same. The login request includes a fixed
`return_to=/arcade.html` option so Discord sign-in returns to the arcade.
The callback now verifies OAuth state and the shared session parser enforces
its existing 30-day lifetime. In-progress logins started before this update
must be restarted once because they have no state cookie.

## Live chat behavior and limits

- Chat requires an authenticated, current member of your configured server
  **and the existing booster/subscription access**. This change does not
  change your pricing or access policy. Games themselves are public assets.
- Both read and send routes enforce the channel allowlist and the user's
  Discord role/member overwrites. Timeouts and membership screening prevent
  sending. Age-restricted channels, threads, forums, and voice are excluded.
- Discord account login does not authorize posting as a normal user. Each
  website message is a **bot message**, prefixed with the member's display
  name and an unpinged user reference for attribution. All mentions are
  disabled for website sends. No user tokens are stored or automated.
- The last 50 messages refresh every five seconds. This updates edits and
  removals within that window; there is no older-history pager, typing status,
  reaction UI, voice, upload composer, or native-user impersonation.
- Metadata and recent messages are cached for five seconds. Membership is
  rechecked for each request. Channel-permission changes can take up to five
  seconds to appear. API errors fail closed, with rate-limit backoff.
- Rate limiting, send locks, and website slow mode are process-local, matching
  the current `Procfile`'s single worker. Use shared storage/Gateway updates
  before scaling to multiple workers or instances. Website slow mode tracks
  website sends, not the member's separate native Discord sends. Bot-relayed
  messages also should not be assumed to inherit native-user AutoMod behavior.
- Existing archive and billing APIs are unchanged apart from the shared
  OAuth state and session-expiry improvements. The new chat router does not
  write to the archive database. The pre-existing archiver may independently
  archive messages posted by the bridge like other Discord messages.
- Third-party games are explicitly owner-configured trusted iframe content.
  If you host untrusted games in future, isolate them on a separate origin
  and review the iframe sandbox policy before adding them.

## Add more games

Edit `arcade-src/games.json`:

```json
[
  {
    "id": "my-next-game",
    "title": "My Next Game",
    "url": "https://YOUR-GITHUB-NAME.github.io/YOUR-GAME/"
  }
]
```

Then run Node 22+:

```sh
node arcade-src/build.mjs
```

Commit the source changes and regenerated `arcade.html` and
`games/helicopter.html`. Games must allow iframe embedding. Switching games
does not replace the chat connection. The helicopter is the single included
game; other cards are added only when you configure actual game URLs.

To embed just the game in another existing page:

```html
<iframe
  src="https://www.discordchats.com/games/helicopter.html"
  title="Helicopter"
  style="width:100%;aspect-ratio:16/10;border:0"
  allow="fullscreen"
  allowfullscreen
></iframe>
```

## Verification

```sh
python -m unittest test_arcade -v
node --test arcade-src/game.test.mjs
```

Tests use mocked Discord HTTP responses; they never send real messages.
Covered: membership, paid/booster gate, wrong server/channel, channel
overwrites, timeout/screening, CSRF/origin checks, slow mode, Discord rate
limits, session expiry, OAuth state, flight/collision mechanics, rendering
API execution, pause/restart, and generated-page script validity.

Browser visual QA and live Discord OAuth/message testing have not been run
in this environment. Complete the deployment checks above before public use.

## Reference documentation

- Discord OAuth2: https://docs.discord.com/developers/topics/oauth2
- Discord permissions: https://docs.discord.com/developers/topics/permissions
- Message API and content intent: https://docs.discord.com/developers/resources/message
- GitHub Pages static hosting: https://docs.github.com/en/pages/getting-started-with-github-pages/what-is-github-pages
