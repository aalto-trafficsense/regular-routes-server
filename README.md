#regular-routes-server

##Introduction

"Learning Regular Routes" -server project contains the server-side code for the "learning regular routes"-project.

##Setting up a new client-server package

This is the procedure for setting up a new client-server pair to run `regularroutes`.

1. Server setup: Install and initialize the server as detailed in the [regular-routes-devops](https://github.com/aalto-trafficsense/regular-routes-devops) readme.
1. The chef initialisation script used by the procedure in devops clones the designated branch of this repository as well
1. Initialize the `users`, `devices` and `device_data` tables as explained [here](https://github.com/aalto-trafficsense/regular-routes-server/tree/master/sql_admin). 
1. Client setup: Build a corresponding client as detailed in the [regular-routes-client](https://github.com/aalto-trafficsense/regular-routes-client) readme.

##Setting up a new server development environment

This is the procedure to set up an environment for developing server software on any local machine.

1. Install [postgresql](http://www.postgresql.org/) and [postgis](http://postgis.net/). Available for Debian Linuxes (Ubuntu) via `apt-get`, and [homebrew](http://brew.sh/) on Mac OS X.
1. Create a regularroutes database - empty or populated - as [instructed] (https://github.com/aalto-trafficsense/regular-routes-server/tree/master/sql_admin). Leave a postgresql server running (`postgres -D rrdb`, where `rrdb` is the path to the directory with your database).  
1. Clone this repo to a location where development is to be carried out. Go (`cd`) into your repository root folder and start a local branch (`git branch my_test`) if practical.
1. Create a `regularroutes.cfg` file (it is listed in .gitignore, so shouldn't spread out to github) with the following contents:

          SECRET_KEY = 'secretkey'
          SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://regularroutes:qwerty@localhost/postgres'
          MAPS_API_KEY = 'INSERT_Browser_API_key_from_Google_Developers_Console'
          APPLICATION_ROOT = '/api'
          AUTH_REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

    Some explanations:
    * `SQLALCHEMY_DATABASE_URI`: `qwerty` is the password for the `regularroutes` role created [here](https://github.com/aalto-trafficsense/regular-routes-server/blob/master/sql_admin/init_rr.sql).
    * On the Google Developers Console, create a project (if not already created) and enable `Google Maps JavaScript API` as explained in [devops readme](https://github.com/aalto-trafficsense/regular-routes-devops).
    * Still on the Console, create the `Browser API key` also according to [devops readme](https://github.com/aalto-trafficsense/regular-routes-devops). Under `Accept requests from these HTTP referrers (web sites)` enter `http://localhost:5000`. Press `Save`
    * From the generated `Browser API key`, copy the `API key` value into `MAPS_API_KEY` in your `regularroutes.cfg` file shown above.

1. Optional: To save a `client_secrets.json` file to the root of your repo, follow again the instruction in the [devops readme](https://github.com/aalto-trafficsense/regular-routes-devops). This file is used in mobile client authentication, but to access this local dev environment from a mobile client also a web server and a routable IP address for the local machine are needed, not covered by these setup instructions.
1. Install and run [PyCharm IDE](https://www.jetbrains.com/pycharm/) for web server / flask development (_Note: There is an educational license available for Intellij-Idea and PyCharm ultimate versions with aalto email._)
1. `File` `Open` your regular-routes-repository root folder. Some package installations should proceed based on the `requirements.txt` file in the folder.
1. Install [Flask-classy](https://pythonhosted.org/Flask-Classy/)  (REST extension package; found from python repo & PyCharm package download list). Under PyCharm installations are done under `Preferences` `Project` `Project Interpreter`.
1. Install any other packages, which might be missing (MJR: In my case psycopg2 refused to install from the `requirements.txt`, so installed it manually from PyCharm).
1. `Run 'regular-routes-server'`
1. If no major problems occurred in startup, open a browser window and test. E.g. `http://localhost:5000/maintenance/duplicates` should receive a `0 duplicate data points were deleted` response. Other commands in the [command reference](https://github.com/aalto-trafficsense/regular-routes-server/wiki/Command-Reference).

##Python virtualenv

Basically [virtualenv](https://virtualenv.pypa.io/en/latest/virtualenv.html) and pip should be the only global python packages. Virtualenv isolates the site-packages for each separate project, making it easier to maintain them. Here's a guide for OSX.
  - install virtualenv
    - e.g. via pip, easy_install, apt-get
    $ sudo easy_install virtual env
    $ sudo pip install virtualenv
  - init virtualenv for the learning regular routes project
    - go to the regular-routes-server folder
    $ virtualenv venv
  - you need to activate the virtualenv before you kick off the server
    - go to the regular-routes-server folder
    $ source venv/bin/activate


