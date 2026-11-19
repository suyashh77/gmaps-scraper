# Two million reviews later: what I learned building a map of every grocery store in North Carolina

Where you live predicts how long you live. In 2016 Raj Chetty and colleagues showed this with 1.4 billion
U.S. tax records: at the same income, people in some places die years earlier than people in others. The
places with shorter lives have more smoking and more obesity, and the local food environment is an obvious suspect.[^1]

Then in 2019 Hunt Allcott and colleagues tested the obvious fix. When a new supermarket opened nearby, or a
family moved to a neighborhood with better stores, diets barely changed. By their estimate, giving
low-income households the same stores and prices as wealthy ones would close only about a tenth of the
nutrition gap.[^2]

So place matters, but putting a store closer doesn't help much. I don't think these findings contradict
each other. I think the problem is that food access almost always means distance: more than a mile to a
supermarket and you live in a food desert. Back in 1981 Penchansky and Thomas described access to care
with five dimensions: availability, accessibility, affordability, accommodation and acceptability.[^3]
Food researchers later adopted the same framework.[^4] Distance covers one of the five. A store can be
close and still too expensive, badly run, or somewhere you don't feel welcome. A new supermarket doesn't
change any of that.

People do write about those things, though. They write about them on Google Maps. That's the idea behind
the dissertation this project supports at NC State: measure food access through what people say about
their stores. A pilot in Raleigh showed it works. My job for the past year was to collect the data for the
whole state.

## Challenge 1: nobody knows how many grocery stores North Carolina has

I expected the hard part to be scraping. It was deciding what a grocery store is.

No single list exists. USDA's list of SNAP retailers is the most trustworthy, because a person verified
every address, but it only covers stores that take food stamps. Overture Maps has far more stores and
cleaner names. OpenStreetMap catches small rural shops and tiendas that commercial lists miss. I pulled all
three, checked them against Data Axle, Dewey (SafeGraph) Places and Census business counts, and ended up with
91,461 records.

The same store appears in several sources under different spellings and slightly different coordinates.
I wrote a matcher, geomatch, that links two records only when they are close together and have similar
names. One early lesson: I couldn't trust Google's own store ID on its own, because the same ID sometimes
pointed at two different stores. Those 348 cases went to a person instead of being merged.

After matching, I had 36,820 food retailers. I searched about 16,750 of them on Google Maps by name and
address, recorded their Google ID, and scraped the category and subcategory Google gives them. Then I sat
down and labeled. Close to 300 category rulings, several rounds of manual checks, and a few rules I had to
defend. Chains are in automatically. Gas stations, closed stores, farmers' markets and produce stands are
out. So are small general-purpose shops that carry a shelf of groceries, and restaurants that sell a few
items on the side. Leaving those in would have doubled the list with places most people wouldn't call a
grocery store.

The final list is 4,442 stores. It is weaker in rural areas, and I want to say that plainly: the smallest
rural stores are exactly the ones every map misses, and they matter most for food access.

## Challenge 2: Google Maps fights back, quietly

The scraper is a Python program that drives a real Chrome browser. It opens a store, scrolls through its
reviews and saves them. I ran it on NC State machines, one per machine so Google wouldn't block it. Almost
every serious bug was one that produced plausible-looking data.

- **Sponsored ads.** The first version searched by store name. Google often put a paid listing at the top,
  the scraper clicked it, and I collected reviews for whichever store bought the ad, often another location
  of the same chain. A food-desert map built on that data would have shown problems that weren't there. The
  fix was to open every store by its Google ID and check that the page is the right store before reading anything.
- **Reviews that change their ID.** Google gives each review a new ID every session. Scrape a store twice and
  it looks like it has twice the reviews. Duplicates are now detected by reviewer, rating and text.
- **Failures that look like success.** Some stores came back "complete" with zero reviews. Sometimes the
  review panel stopped scrolling without any error. The scraper now proves the page is moving and won't mark
  a store done without evidence.
- **Logins that don't travel.** Google ties a login to the computer that made it. Copied to a lab machine, it
  signed out within minutes.
- **Stores that are too big.** A busy Walmart has 9,000 reviews. Collecting them all takes most of a day, and
  for some stores Google simply stops showing older reviews. I cap each store at 1,500.

## Challenge 3: deciding when "done" means done

A store's review count on Google is a moving target. People delete reviews, and Google hides some. So I
needed a rule, not a perfect match: a store is complete if it is missing at most 10 reviews when it has 100
or fewer, 20 above 100, 50 above 500, and 100 above 1,500. Without that rule I would still be rescraping
stores for the last three reviews Google no longer shows anyone.

## The outcome

- **4,442** grocery stores across **590** North Carolina cities and towns
- **1,941,538** Google reviews, **977,398** of them with written text
- **4,400** stores complete. The other 42 are short by 21 to 25 reviews each (two wait on a manual check),
  955 reviews in total.
- About 250 of the largest stores hold their newest 1,500-plus reviews rather than their whole history, so
  any analysis of those stores has to treat older reviews as missing.

Every step is a script that can be rerun. Nothing was deleted to make a number look better: reviews that
were set aside live in their own table with a reason attached. The final database is frozen with a
checksum so anyone can confirm it hasn't changed.

## What I'd tell myself a year ago

Spend the first month on the store list, not the scraper. Treat every source as wrong somewhere, and
decide in advance which one you trust for what. Never trust a number that came out of a script nobody
checked by hand. And write down every decision with a date and a reason, because six months later you will
not remember why store 4868 has 3,064 reviews on a target of 1,500.

The next step is the analysis itself: reading two million reviews for what they say about affordability,
acceptability and the other dimensions that distance never measured.

[^1]: Chetty R, Stepner M, Abraham S, et al. The association between income and life expectancy in the United States, 2001–2014. *JAMA*. 2016;315(16):1750–1766.
[^2]: Allcott H, Diamond R, Dubé J-P, Handbury J, Rahkovsky I, Schnell M. Food deserts and the causes of nutritional inequality. *Quarterly Journal of Economics*. 2019;134(4):1793–1844.
[^3]: Penchansky R, Thomas JW. The concept of access: definition and relationship to consumer satisfaction. *Medical Care*. 1981;19(2):127–140.
[^4]: Caspi CE, Sorensen G, Subramanian SV, Kawachi I. The local food environment and diet: a systematic review. *Health & Place*. 2012;18(5):1172–1187.
