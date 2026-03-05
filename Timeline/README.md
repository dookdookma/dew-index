# Mixed Reddit + Nitter RSS Feed Starter

This starter takes the exact sources you supplied and generates one merged RSS feed.

## Included sources

### Nitter / X accounts
- https://nitter.poast.org/Logo_Daedalus/rss
- https://nitter.poast.org/LapsusLima/rss
- https://nitter.poast.org/newcriterion/rss
- https://nitter.poast.org/deankissick/rss
- https://nitter.poast.org/laserboat999/rss
- https://nitter.poast.org/mcrumps/rss

### Reddit subreddits
- https://www.reddit.com/r/OutOfTheLoop/.rss
- https://www.reddit.com/r/MemeEconomy/.rss
- https://www.reddit.com/r/KnowYourMeme/.rss
- https://www.reddit.com/r/socialmedia/.rss
- https://www.reddit.com/r/TheoryOfReddit/.rss
- https://www.reddit.com/r/SubredditDrama/.rss
- https://www.reddit.com/r/decadeology/.rss
- https://www.reddit.com/r/Futurology/.rss

## What it does
- fetches all feeds
- normalizes items into one schema
- includes post text in item descriptions
- labels items as `[X]` or `[Reddit]`
- sorts everything by publish time
- deduplicates by link/GUID
- outputs a merged RSS 2.0 file

## Caveat
`nitter.poast.org` may return 403 errors for some RSS readers and clients. The script sends a desktop-style User-Agent to improve compatibility, but you should still treat the Nitter instance as swappable.

If `nitter.poast.org` becomes flaky, replace only the host while preserving the same path pattern:

- `https://INSTANCE/Logo_Daedalus/rss`
- `https://INSTANCE/LapsusLima/rss`
- etc.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/build_feed.py --config config/sources.json --output dist/feed.xml
```

## Output
The merged file will be written to:

```bash
dist/feed.xml
```

## Hosting options
- serve `dist/feed.xml` from any static host
- regenerate on a cron job every 15 minutes
- keep the same public URL for your RSS readers

## Suggested cron

```bash
*/15 * * * * cd /path/to/social_feed_starter && /path/to/.venv/bin/python src/build_feed.py --config config/sources.json --output dist/feed.xml
```
