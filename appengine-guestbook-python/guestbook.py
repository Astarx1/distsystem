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
import json
import unicodedata
import time
import copy

def get_user(request):
    user = users.get_current_user()
    url = None
    url_linktext = None
    nickname = None

    if user:
        url = user.create_logout_url(self.request.uri)
        nickname = user.nickname()
        url_linktext = 'Logout'
    else:
        url = user.create_login_url(self.request.uri)
        url_linktext = 'Login'

    return user, url, url_linktext, nickname


def remove_accent(input_str):
    if input_str is None:
        return
    else:
        nfkd_form = unicodedata.normalize('NFKD', unicode(input_str))
        return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])


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

def cart_key():
    return ndb.Key('Cart', 'cart_storage')

def sales_key():
    return ndb.Key('Sales', 'sales_storage')

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
    wine_price = ndb.StringProperty(indexed=False)


class CartEntry(ndb.Model):
    wine_id = ndb.IntegerProperty()
    owner = ndb.StringProperty()

    @classmethod
    def get_from_nickname(cls, ancestor, name):
        return cls.query(CartEntry.owner==name, ancestor=ancestor).fetch()

    # @classmethod
    # def get_from_id(cls, ancestor, entry_id)


class Sales(ndb.Model):
    wines_id = ndb.StringProperty()
    owner = ndb.StringProperty()
    timestamp = ndb.IntegerProperty()
    price = ndb.IntegerProperty()


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

        #if users.get_current_user():
        #    greeting.author = Author(
        #            identity=users.get_current_user().user_id(),
        #            email=users.get_current_user().email())

        if self.request.get('wine_type') != '' and self.request.get('wine_country') != '' and self.request.get('wine_region') != '' \
            and self.request.get('wine_variety') != '' and self.request.get('wine_winery') != '' and self.request.get('wine_year') != '' and self.request.get('wine_price') != '':
            greeting.wine_type = remove_accent(self.request.get('wine_type'))
            greeting.wine_country = remove_accent(self.request.get('wine_country'))
            greeting.wine_region = remove_accent(self.request.get('wine_region'))
            greeting.wine_variety = remove_accent(self.request.get('wine_variety'))
            greeting.wine_winery = remove_accent(self.request.get('wine_winery'))
            greeting.wine_year = remove_accent(self.request.get('wine_year'))
            greeting.wine_price = remove_accent(self.request.get('wine_price'))

            greeting.put()
            self.redirect('/?new_wine=true')
        else: 
            self.redirect('/?new_wine=false')
# [END guestbook]

class Carthdl(webapp2.RequestHandler):
    def post(self):
        greeting = CartEntry(parent=cart_key())
        user = users.get_current_user()
        url = None
        url_linktext = None
        nickname = None
                            
        if user:
            url = users.create_logout_url(self.request.uri)
            nickname = user.nickname()
            url_linktext = 'Logout'
        else:
            url = users.create_login_url(self.request.uri)
            url_linktext = 'Login'
        
        if self.request.get('wine_id') != '' and nickname != None:
            greeting.wine_id = int(self.request.get('wine_id'))
            greeting.owner = nickname
            greeting.put()
            print("Wine added to cart")

        self.redirect('/')

    def get(self):
        user = users.get_current_user()
        url = None
        url_linktext = None
        nickname = None
         
        if user:
            nickname = user.nickname()
            url_linktext = 'Logout'
        else:
            url = users.create_login_url(self.request.uri)
            url_linktext = 'Login'

        ret = []

        if user:
            greetings_query = CartEntry.get_from_nickname(name=nickname, ancestor=cart_key())
            greetings = greetings_query
        
            wines_query = Wine.query(ancestor=wine_key())
            wines = wines_query.fetch()

            ws = {}
            for w in wines:
                ws[w.key.id()] = {"wine_id":w.key.id(), "type": w.wine_type, "country": w.wine_country, "region": w.wine_region, "variety": w.wine_variety, "winery": w.wine_winery, "year": w.wine_year, "price":w.wine_price}

            already_in_cart = {}
            for w in greetings:
                if w.wine_id in ws and w.wine_id not in already_in_cart:
                    ret.append({"wine": ws[w.wine_id], "cart_entry":[w.key.id()]})
                    already_in_cart[w.wine_id] = len(ret)-1

                elif w.wine_id in ws:
                    ret[already_in_cart[w.wine_id]]["cart_entry"].append(w.key.id())

        self.response.content_type = "application/json"
        self.response.write(json.dumps(ret, ensure_ascii=False))

    def delete(self): 
        user = users.get_current_user()
        url = None
        url_linktext = None
        nickname = None

        if user:
            url = users.create_logout_url(self.request.uri)
            nickname = user.nickname()
            url_linktext = 'Logout'
                
            if self.request.get('checkout') != '':
                user = users.get_current_user()
                url = None
                url_linktext = None
                nickname = None
                            
                if user:
                    url = users.create_logout_url(self.request.uri)
                    nickname = user.nickname()
                    url_linktext = 'Logout'
                else:
                    url = users.create_login_url(self.request.uri)
                    url_linktext = 'Login'
        
                if self.request.get('wine_id') != '' and nickname != None:
                    greeting.wine_id = int(self.request.get('wine_id'))
                    greeting.owner = nickname
                    greeting.put()

                entries = CartEntry.get_from_nickname(name=nickname, ancestor=cart_key())
                greeting = Sales(parent=sales_key())
                greeting.owner = nickname
                ids_wine_to_save = []
                for e in entries:
                    ids_wine_to_save.append(str(e.wine_id))
                    e.key.delete()
                greeting.wines_id = ",".join(ids_wine_to_save)
                greeting.timestamp = int(time.time())
                greeting.put()

            elif self.request.get('id') != '':
                entries = CartEntry.get_from_nickname(name=nickname, ancestor=cart_key())
                c = CartEntry.get_by_id(long(self.request.get('id')), parent=cart_key())
                if c:
                    c.key.delete()

        else:
            url = users.create_logout_url(self.request.uri)
            url_linktext = 'Login'

        self.redirect("/cart")


class SalesAPI(webapp2.RequestHandler):
    def get(self):
        greeting = Sales.query(ancestor=sales_key()).fetch()
        wines = Wine.query(ancestor=wine_key()).fetch()
        ws = {}
        for w in wines:
            ws[w.key.id()] = {"wine_id":w.key.id(), "type": w.wine_type, "country": w.wine_country, "region": w.wine_region, "variety": w.wine_variety, "winery": w.wine_winery, "year": w.wine_year, "price":w.wine_price, "number":0}

        ret = []
        for g in greeting:
            if g.wines_id is None:
                continue
            s = []
            s = {"buyer": g.owner, "timestamp":g.timestamp, "wines":[]}
            ws_id = g.wines_id.split(",") 
            already_in_wines = {}
            
            for bb in ws_id:
                if bb is u'':
                    continue
                elif long(bb) in ws and long(bb) not in already_in_wines:
                    s["wines"].append(copy.deepcopy(ws[long(bb)]))
                    already_in_wines[long(bb)] = len(s["wines"]) - 1
                    s["wines"][already_in_wines[long(bb)]]["number"] = 1
                elif long(bb) in ws:
                    s["wines"][already_in_wines[long(bb)]]["number"] += 1
            
            ret.append(s)
        
        self.response.content_type = "application/json"
        self.response.write(json.dumps(ret, ensure_ascii=False))


class NewEntry(webapp2.RequestHandler):
    def get(self):
        template_values = {}
        template = JINJA_ENVIRONMENT.get_template('new_entry.html')
        self.response.write(template.render(template_values))

class Display(webapp2.RequestHandler):
    def get(self):
        wine_category = remove_accent(self.request.get('wine_type'))
        wine_country = remove_accent(self.request.get('wine_country'))
        wine_region = remove_accent(self.request.get('wine_region'))
        wine_variety = remove_accent(self.request.get('wine_variety'))
        wine_winery = remove_accent(self.request.get('wine_winery'))
        wine_year = remove_accent(self.request.get('wine_year'))
        
        greetings_query = Wine.query(ancestor=wine_key())
        greetings = greetings_query.fetch()

        wines = []

        for w in greetings:
            result = True
            if wine_category is not None and wine_category is not '' and wine_category is not 'all':
                if w.wine_type is not None:
                    if wine_category.lower != w.wine_type.lower() and wine_category.lower() not in w.wine_type.lower():
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
                            result = False

                        if str(wine_year) not in str(w.wine_year):
                            result = False
                        else:
                            print(str(wine_year) + " in " + str(w.wine_year))
                    else:
                        result = False
            except:
                pass

            if result:
                wines.append(w)
        
        message = ''
        if len(wines) == 0:
            message = 'No wine found.'

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


class Cart(webapp2.RequestHandler):
    def get(self):
        template_values={}
        template = JINJA_ENVIRONMENT.get_template('cart.html')
        self.response.write(template.render(template_values))

class SalesView(webapp2.RequestHandler):
    def get(self):
        template_values = {}
        template = JINJA_ENVIRONMENT.get_template('sales.html')
        self.response.write(template.render(template_values))

# [START app]
app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/enter', NewEntry),
    ('/add', NewWine),
    ('/display', Display),
    ('/search', Search),
    ('/cart', Cart),
    ('/api/sales', SalesAPI),
    ('/api/cart', Carthdl),
    ('/sales', SalesView)
], debug=True)
# [END app]
