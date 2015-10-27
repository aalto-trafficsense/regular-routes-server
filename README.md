#regular-routes-server

##Introduction

"Learning Regular Routes" -server project contains the server-side code for the "learning regular routes"-project.

##Checklist for setting up a server development environment

- server setup: Install and initialize the server as detailed in the [reguler-routes-devops](https://github.com/aalto-trafficsense/regular-routes-devops) readme.
- client setup: Build a corresponding client as detailed in the [regular-routes-client](https://github.com/aalto-trafficsense/regular-routes-client) readme.

- server development: Install [PyCharm IDE](https://www.jetbrains.com/pycharm/) for web server / flask development 
- server development: Install [Flask-classy](https://pythonhosted.org/Flask-Classy/)  (REST extension package; found from python repo & PyCharm package download list)

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

_Note: There is an educational license available for Intellij-Idea and PyCharm ultimate versions with aalto email._
