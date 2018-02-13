#!/usr/bin/env python

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START imports]
import os
import urllib

from google.appengine.api import users
from google.appengine.ext import ndb

import jinja2
import webapp2

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)
# [END imports]

DEFAULT_GUESTBOOK_NAME = 'default_guestbook'


# We set a parent key on the 'Greetings' to ensure that they are all
# in the same entity group. Queries across the single entity group
# will be consistent. However, the write rate should be limited to
# ~1/second.

def guestbook_key(guestbook_name=DEFAULT_GUESTBOOK_NAME):
    """Constructs a Datastore key for a Guestbook entity.

    We use guestbook_name as the key.
    """
    return ndb.Key('Guestbook', guestbook_name)


def wine_key():
    return ndb.Key('Wines', 'wine_storage')


# [START greeting]
class Author(ndb.Model):
    """Sub model for representing an author."""
    identity = ndb.StringProperty(indexed=False)
    email = ndb.StringProperty(indexed=False)

class Wine(ndb.Model):
    wine_type = ndb.StringProperty(indexed=True)
    wine_country = ndb.StringProperty(indexed=False)
    wine_region = ndb.StringProperty(indexed=False)
    wine_variety = ndb.StringProperty(indexed=False)
    wine_winery = ndb.StringProperty(indexed=False)
    wine_year = ndb.StringProperty(indexed=False)

class Greeting(ndb.Model):
    """A main model for representing an individual Guestbook entry."""
    author = ndb.StructuredProperty(Author)
    content = ndb.StringProperty(indexed=False)
    date = ndb.DateTimeProperty(auto_now_add=True)
# [END greeting]


# [START main_page]
class MainPage(webapp2.RequestHandler):

    def get(self):
        guestbook_name = self.request.get('guestbook_name',
                                          DEFAULT_GUESTBOOK_NAME)
        greetings_query = Greeting.query(
            ancestor=guestbook_key(guestbook_name)).order(-Greeting.date)
        greetings = greetings_query.fetch(10)

        user = users.get_current_user()
        if user:
            url = users.create_logout_url(self.request.uri)
            url_linktext = 'Logout'
        else:
            url = users.create_login_url(self.request.uri)
            url_linktext = 'Login'

        template_values = {
            'user': user,
            'greetings': greetings,
            'guestbook_name': urllib.quote_plus(guestbook_name),
            'url': url,
            'url_linktext': url_linktext,
        }

        template = JINJA_ENVIRONMENT.get_template('index.html')
        self.response.write(template.render(template_values))
# [END main_page]

# [START guestbook]
class NewWine(webapp2.RequestHandler):
    def post(self):
        # We set the same parent key on the 'Greeting' to ensure each
        # Greeting is in the same entity group. Queries across the
        # single entity group will be consistent. However, the write
        # rate to a single entity group should be limited to
        # ~1/second.
        greeting = Wine(parent=wine_key())

        if users.get_current_user():
            greeting.author = Author(
                    identity=users.get_current_user().user_id(),
                    email=users.get_current_user().email())

        greeting.wine_type = self.request.get('wine_type')
        greeting.wine_country = self.request.get('wine_country')
        greeting.wine_region = self.request.get('wine_region')
        greeting.wine_variety = self.request.get('wine_variety')
        greeting.wine_winery = self.request.get('wine_winery')
        greeting.wine_year = self.request.get('wine_year')

        print("New wine from - " + greeting.wine_winery + "(" + self.request.get('wine_winery') + ")")

        greeting.put()

        self.redirect('/')
# [END guestbook]

class NewEntry(webapp2.RequestHandler):
    def get(self):
        template_values = {}
        template = JINJA_ENVIRONMENT.get_template('new_entry.html')
        self.response.write(template.render(template_values))

class Display(webapp2.RequestHandler):
    def get(self):
        wine_category = self.request.get('wine_type')
        wine_country = self.request.get('wine_country')
        wine_region = self.request.get('wine_region')
        wine_variety = self.request.get('wine_variety')
        wine_winery = self.request.get('wine_winery')
        wine_year = self.request.get('wine_year')
        
        greetings_query = Wine.query(ancestor=wine_key())
        greetings = greetings_query.fetch()
        print("We have been asked "+ wine_category +" wines :" + str(greetings))
        wines = []

        for w in greetings:
            result = True
            if wine_category is not None and wine_category is not '' and wine_category is not 'all':
                if w.wine_type is not None:
                    if wine_category.lower != w.wine_type.lower() and wine_category.lower() not in w.wine_type.lower():
                        print("'" + wine_category.lower() + "' not in '" + w.wine_type.lower() + "'")
                        result = False
                else:
                    result = False
            
            if wine_country is not None and wine_country is not '':
                if w.wine_country is not None:
                    if wine_country.lower() not in w.wine_country.lower():
                        result = False
                else:
                    result = False
            
            if wine_region is not None:
                if w.wine_region is not None:
                    if wine_region.lower() not in w.wine_region.lower():
                        result = False
                else:
                    result = None
            
            if wine_variety is not None:
                if w.wine_variety is not None:
                    if wine_variety.lower() not in w.wine_variety.lower():
                        result = False
                else:
                    result = None
            
            if wine_winery is not None:
                if w.wine_winery is not None:
                    if wine_winery.lower() not in w.wine_winery.lower():
                        result = False
                else:
                    result = False
            
            try:
                if wine_year is not None and int(wine_year) > 0 and wine_year is not '':
                    if w.wine_year is not None:
                        try:
                            int(w.wine_year)
                        except:
                            print("Wine bad year format '" + w.wine_year + "'")
                            result = False

                        if str(wine_year) not in str(w.wine_year):
                            result = False
                        else:
                            print(str(wine_year) + " in " + str(w.wine_year))
                    else:
                        result = False
            except:
                print("Research bad year format : '" + str(wine_year) + "'")


            if result:
                wines.append(w)
        
        message = ''
        print(len(wines))
        if len(wines) == 0:
            message = 'No wine found'

        template_values = {
            'wines': wines[0:10],
            'message': message
        }
        print(message)
        
        template = JINJA_ENVIRONMENT.get_template('display.html')
        self.response.write(template.render(template_values))

class Search(webapp2.RequestHandler):
    def get(self):
        template_values={}
        template = JINJA_ENVIRONMENT.get_template('search.html')
        self.response.write(template.render(template_values))


# [START app]
app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/enter', NewEntry),
    ('/add', NewWine),
    ('/display', Display),
    ('/search', Search)
], debug=True)
# [END app]
