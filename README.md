Python Client for Riak, Take 2
==============================

riak-python-client2 is a rewrite of the official client to bring it up to speed
with the current python practices (such as not using `set` and `get` as well as
`apply`). It's also a redesign in the attempt to make the system more modular
and flexible.

The client will be made up of 2 parts. `riak2` and `riak2.core`. `riak2.core`
provides the very basics to communicate with the riak server and `riak2`
consists of a higher level API that builds on top of `riak2.core`. I have the
HTTP API implemented in `riak2.core`, take a look at the API docs under
`riak2/core/transport.py`.

The current status of riak-python-client2 is that it's incomplete. The higher
level API is still under design and the PBC transport has not been started yet.

Feel free to fork and help out this project. You could also
[![Donate to me to keep this going!](https://www.paypalobjects.com/en_US/i/btn/btn_donate_SM.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=FGWYWWS4CJJFW&lc=CA&item_name=Riakkit&item_number=riakkit&currency_code=CAD&bn=PP%2dDonationsBF%3abtn_donate_SM%2egif%3aNonHosted)
