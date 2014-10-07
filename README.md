regular-routes-server
=====================

#Introduction
"Learning Regular Routes" -server project contains server side code for learning regular routes project


#Setting up development environment checklist

- db: Install Chef tools from regular-routes-devops repo

- client: Install JDK8 + Android SDK 
- client: Install IntelliJ-Idea IDE for android client development 

- server: Install PyCharm IDE for web server / flask development 
- server: Install Flask-classy  (REST extension pacakge; found from python repo & PyCharm package download list) 


#Python virtualenv
Basically virtualenv and pip should be the only global python packages. Virtualenv isolates the site-packages for each separate project, making it 
easier to maintain them. More info at https://virtualenv.pypa.io/en/latest/virtualenv.html. Here's a guide for OSX.
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

note: There is educational license available for Intellij-Idea and PyCharm ultimate versions with aalto email.
