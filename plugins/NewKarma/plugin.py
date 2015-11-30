###
# Copyright (c) 2005, Jeremiah Fincher
# Copyright (c) 2010, James Vega
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions, and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions, and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of the author of this software nor the name of
#     contributors to this software may be used to endorse or promote products
#     derived from this software without specific prior written consent.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
###

import os
import csv

import supybot.conf as conf
import supybot.utils as utils
from supybot.commands import *
import supybot.plugins as plugins
import supybot.ircmsgs as ircmsgs
import supybot.ircutils as ircutils
import supybot.callbacks as callbacks
import supybot.log as log

try:
    import sqlite3
except ImportError:
    from pysqlite2 import dbapi2 as sqlite3 # for python2.4

class SqliteKarmaDB(object):
    def __init__(self, filename):
        self.dbs = ircutils.IrcDict()
        self.filename = filename

    def close(self):
        for db in self.dbs.itervalues():
            db.close()

    def _getDb(self, channel):
        filename = plugins.makeChannelFilename(self.filename, channel)
        if filename in self.dbs:
            return self.dbs[filename]
        if os.path.exists(filename):
            db = sqlite3.connect(filename)
            db.text_factory = str
            self.dbs[filename] = db
            return db
        db = sqlite3.connect(filename)
        db.text_factory = str
        self.dbs[filename] = db
        cursor = db.cursor()
        cursor.execute("""CREATE TABLE karma (
                          id INTEGER PRIMARY KEY,
                          name TEXT,
                          normalized TEXT UNIQUE ON CONFLICT IGNORE,
                          added INTEGER,
                          subtracted INTEGER
                          )""")
        db.commit()
        def p(s1, s2):
            return int(ircutils.nickEqual(s1, s2))
        db.create_function('nickeq', 2, p)
        return db

    def get(self, channel, thing):
        db = self._getDb(channel)
        thing = thing.lower()
        cursor = db.cursor()
        cursor.execute("""SELECT added, subtracted FROM karma
                          WHERE normalized=?""", (thing,))
        results = cursor.fetchall()
        if len(results) == 0:
            return None
        else:
            return map(int, results[0])

    def gets(self, channel, things):
        db = self._getDb(channel)
        cursor = db.cursor()
        normalizedThings = dict(zip(map(lambda s: s.lower(), things), things))
        criteria = ' OR '.join(['normalized=?'] * len(normalizedThings))
        sql = """SELECT name, added-subtracted FROM karma
                 WHERE %s ORDER BY added-subtracted DESC""" % criteria
        cursor.execute(sql, normalizedThings.keys())
        L = [(name, int(karma)) for (name, karma) in cursor.fetchall()]
        for (name, _) in L:
            del normalizedThings[name.lower()]
        neutrals = normalizedThings.values()
        neutrals.sort()
        return (L, neutrals)

    def top(self, channel, limit):
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""SELECT name, added-subtracted FROM karma
                          ORDER BY added-subtracted DESC LIMIT ?""", (limit,))
        return [(t[0], int(t[1])) for t in cursor.fetchall()]

    def bottom(self, channel, limit):
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""SELECT name, added-subtracted FROM karma
                          ORDER BY added-subtracted ASC LIMIT ?""", (limit,))
        return [(t[0], int(t[1])) for t in cursor.fetchall()]

    def rank(self, channel, thing):
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""SELECT added-subtracted FROM karma
                          WHERE name=?""", (thing,))
        results = cursor.fetchall()
        if len(results) == 0:
            return None
        karma = int(results[0][0])
        cursor.execute("""SELECT COUNT(*) FROM karma
                          WHERE added-subtracted > ?""", (karma,))
        rank = int(cursor.fetchone()[0])
        return rank+1

    def size(self, channel):
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""SELECT COUNT(*) FROM karma""")
        return int(cursor.fetchone()[0])

    def increment(self, channel, name):
        db = self._getDb(channel)
        cursor = db.cursor()
        normalized = name.lower()
        cursor.execute("""INSERT INTO karma VALUES (NULL, ?, ?, 0, 0)""",
                       (name, normalized,))
        cursor.execute("""UPDATE karma SET added=added+1
                          WHERE normalized=?""", (normalized,))
        db.commit()

    def decrement(self, channel, name):
        db = self._getDb(channel)
        cursor = db.cursor()
        normalized = name.lower()
        cursor.execute("""INSERT INTO karma VALUES (NULL, ?, ?, 0, 0)""",
                       (name, normalized,))
        cursor.execute("""UPDATE karma SET subtracted=subtracted+1
                          WHERE normalized=?""", (normalized,))
        db.commit()

    def garbageCollect(self, channel, name):
        db = self._getDb(channel)
        cursor = db.cursor()
        normalized = name.lower()
        cursor.execute("""DELETE FROM karma WHERE normalized='%s'""" % (normalized))
        db.commit()

    def most(self, channel, kind, limit):
        if kind == 'increased':
            orderby = 'added'
        elif kind == 'decreased':
            orderby = 'subtracted'
        elif kind == 'active':
            orderby = 'added+subtracted'
        else:
            raise ValueError, 'invalid kind'
        sql = """SELECT name, %s FROM karma ORDER BY %s DESC LIMIT %s""" % \
              (orderby, orderby, limit)
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute(sql)
        return [(name, int(i)) for (name, i) in cursor.fetchall()]

    def clear(self, channel, name):
        db = self._getDb(channel)
        cursor = db.cursor()
        normalized = name.lower()
        cursor.execute("""UPDATE karma SET subtracted=0, added=0
                          WHERE normalized=?""", (normalized,))
        db.commit()

    def dump(self, channel, filename):
        filename = conf.supybot.directories.data.dirize(filename)
        fd = utils.transactionalFile(filename)
        out = csv.writer(fd)
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""SELECT name, added, subtracted FROM karma""")
        for (name, added, subtracted) in cursor.fetchall():
            out.writerow([name, added, subtracted])
        fd.close()

    def load(self, channel, filename):
        filename = conf.supybot.directories.data.dirize(filename)
        fd = file(filename)
        reader = csv.reader(fd)
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""DELETE FROM karma""")
        for (name, added, subtracted) in reader:
            normalized = name.lower()
            cursor.execute("""INSERT INTO karma
                              VALUES (NULL, ?, ?, ?, ?)""",
                           (name, normalized, added, subtracted,))
        db.commit()
        fd.close()

class SqliteAliasDB(object):
    def __init__(self, filename):
        self.dbs = ircutils.IrcDict()
        self.filename = filename

    def close(self):
        for db in self.dbs.itervalues():
            db.close()

    def _getDb(self, channel):
        filename = plugins.makeChannelFilename(self.filename, channel)
        if filename in self.dbs:
            return self.dbs[filename]
        if os.path.exists(filename):
            db = sqlite3.connect(filename)
            db.text_factory = str
            self.dbs[filename] = db
            return db
        db = sqlite3.connect(filename)
        db.text_factory = str
        self.dbs[filename] = db
        cursor = db.cursor()
        cursor.execute("""CREATE TABLE alias (
                          id INTEGER PRIMARY KEY,
                          name TEXT,
                          normalized TEXT,
                          aliases TEXT
                          )""")
        db.commit()
        def p(s1, s2):
            return int(ircutils.nickEqual(s1, s2))
        db.create_function('nickeq', 2, p)
        return db

    def get_aliases(self, channel, thing):
        db = self._getDb(channel)
        thing = thing.lower()
        cursor = db.cursor()
        cursor.execute("""SELECT aliases FROM alias
                          WHERE normalized=?""", (thing.lower(),))
        results = cursor.fetchall()
        if len(results) == 0:
            return []
        else:
            return [str(x[0]) for x in results]

    def get(self, channel, thing):
        db = self._getDb(channel)
        thing = thing.lower()
        cursor = db.cursor()
        cursor.execute("""SELECT normalized FROM alias
                          WHERE aliases like ?""", (thing.lower(),))
        results = cursor.fetchall()
        if len(results) == 0:
            return []
        else:
            return [str(x[0]) for x in results]

    def alias(self, channel, name, alias):
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""INSERT INTO alias VALUES (NULL, ?, ?, ?)""", (name, name.lower(), alias,))
        db.commit()

    def unalias(self, channel, name, alias):
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""DELETE FROM alias where normalized=? AND aliases=?""", (name.lower(), alias,))
        db.commit()

    def dump(self, channel, filename):
        filename = conf.supybot.directories.data.dirize(filename)
        fd = utils.transactionalFile(filename)
        out = csv.writer(fd)
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""SELECT name, aliases FROM alias""")
        for (name, aliases) in cursor.fetchall():
            out.writerow([name, aliases])
        fd.close()

    def load(self, channel, filename):
        filename = conf.supybot.directories.data.dirize(filename)
        fd = file(filename)
        reader = csv.reader(fd)
        db = self._getDb(channel)
        cursor = db.cursor()
        cursor.execute("""DELETE FROM alias""")
        for (name, aliases) in reader:
            normalized = name.lower()
            cursor.execute("""INSERT INTO alias
                              VALUES (NULL, ?, ?, ?)""",
                           (name, normalized, aliases,))
        db.commit()
        fd.close()


KarmaDB = plugins.DB('Karma',
                     {'sqlite3': SqliteKarmaDB})
AliasDB = plugins.DB('KarmaAliases',
                     {'sqlite3': SqliteAliasDB})

class NewKarma(callbacks.Plugin):
    callBefore = ('Factoids', 'MoobotFactoids', 'Infobot')
    def __init__(self, irc):
        self.__parent = super(NewKarma, self)
        self.__parent.__init__(irc)
        self.db = KarmaDB()
        self.alias_db = AliasDB()

    def die(self):
        self.__parent.die()
        self.db.close()
        self.alias_db.close()

    def _normalizeThing(self, thing):
        assert thing
        if thing[0] == '(' and thing[-1] == ')':
            thing = thing[1:-1]
        return thing

    def _respond(self, irc, channel, message=None):
        if self.registryValue('response', channel):
	    if message:
                irc.reply(message, prefixNick=False)
            else:
                irc.replySuccess()
        else:
            irc.noReply()

    def _parseKarmaMessage(self, name, total, channel, originalname, direction):
        if direction == "up":
            message = self.registryValue('karmaMessageUp', channel)
        elif direction == "down":
            message = self.registryValue('karmaMessageDown', channel)
        elif direction == "none":
            message = self.registryValue('karmaMessageNone', channel)
        else:
            message = "I have no idea what you are talking about."
        if total == 1 or total == -1:
            message = message.replace('points', 'point')
        if originalname:
            name = "%s (%s)" % (originalname, name)
        return  message.replace('USER', name).replace('TOTAL', str(total))

    def _doAlias(self, irc, channel, things):
      name = things.split('is also known as')[0].split()[-1]
      alias = things.split('is also known as')[1].split()[0]
      self.alias_db.alias(channel, name, alias)
      irc.reply("%s is also %s, got it!" % (name, alias))

    def _doUnalias(self, irc, channel, things):
      name = things.split('is no longer known as')[0].split()[-1]
      alias = things.split('is no longer known as')[1].split()[0]
      self.alias_db.unalias(channel, name, alias)
      irc.reply("Who?  I've forgotten that %s was ever %s!" % (name, alias))

    def _doKarma(self, irc, channel, things):
      for thing in things.split():
        originalthing = None
        #if thing.endswith('++'):
        if "++" in thing:
            thing = thing.split("++")[0]
            if thing:
              #see if what we are incrementing is an alias for someone
              aliasfor = self.alias_db.get(channel, self._normalizeThing(thing))
              if aliasfor:
                originalthing = thing
                thing = aliasfor
              if type(thing) != list:
                thing = [thing]
              for athing in thing:
                #Honor allowSelfRating unless this is a group alias
                if ircutils.strEqual(athing, irc.msg.nick) and \
                   not self.registryValue('allowSelfRating', channel) and \
                   len(thing) == 1:
                  irc.error('You\'re not allowed to adjust your own karma.')
                else:
                  self.db.increment(channel, self._normalizeThing(athing))
                  t = self.db.get(channel, athing)
                  if t is None:
                    total = 0
                  else:
                    (added, subtracted) = t
                    total = added - subtracted
                  if total == 0:
                    self._respond(irc, channel, self._parseKarmaMessage(athing, total, channel, originalthing, "none"))
	            self.db.garbageCollect(channel, athing)
                  else:
                    self._respond(irc, channel, self._parseKarmaMessage(athing, total, channel, originalthing, "up"))
        #decrement unless some person has "--" in their name in channel
        elif "--" in thing and thing not in irc.state.channels[channel].users:
            #Hack for users with "--" in their name being given negative karma
            if thing[0:-2] in irc.state.channels[channel].users:
                thing = thing[0:-2]
            else:
                thing = thing.split("--")[0]
            if thing:
              #see if what we are incrementing is an alias for someone
              aliasfor = self.alias_db.get(channel, self._normalizeThing(thing))
              if aliasfor:
                originalthing = thing
                thing = aliasfor
              if type(thing) != list:
                thing = [thing]
              for athing in thing:
                #Honor allowSelfRating unless this is a group alias
                if ircutils.strEqual(athing, irc.msg.nick) and \
                   not self.registryValue('allowSelfRating', channel) and \
                   len(thing) == 1:
                  irc.error('You\'re not allowed to adjust your own karma.')
                else:
                  self.db.decrement(channel, self._normalizeThing(athing))
                  t = self.db.get(channel, athing)
                  if t is None:
                    total = 0
                  else:
                    (added, subtracted) = t
                    total = added - subtracted
                  if total == 0:
                    self._respond(irc, channel, self._parseKarmaMessage(athing, total, channel, originalthing, "none"))
	            self.db.garbageCollect(channel, athing)
                  else:
                    self._respond(irc, channel, self._parseKarmaMessage(athing, total, channel, originalthing, "down"))

    def invalidCommand(self, irc, msg, tokens):
        channel = msg.args[0]
        if not irc.isChannel(channel):
            return
        if tokens[-1][-2:] in ('++', '--'):
            thing = ' '.join(tokens)
            self._doKarma(irc, channel, thing)

    def doPrivmsg(self, irc, msg):
        # We don't handle this if we've been addressed because invalidCommand
        # will handle it for us.  This prevents us from accessing the db twice
        # and therefore crashing.
        if not (msg.addressed or msg.repliedTo):
            channel = msg.args[0]
            if irc.isChannel(channel) and \
               not ircmsgs.isCtcp(msg) and \
               self.registryValue('allowUnaddressedKarma', channel):
                irc = callbacks.SimpleProxy(irc, msg)
                thing = msg.args[1].rstrip()
                if '++' in thing or '--' in thing:
                    self._doKarma(irc, channel, thing)
                if 'is also known as' in thing:
                    self._doAlias(irc, channel, thing)

                if 'is no longer known as' in thing:
                    self._doUnalias(irc, channel, thing)

    def showaliases(self, irc, msg, args, channel, name):
        """[<channel>] <word>

        Lists the karmaaliases for a given word.
        """
        if name:
          name = name[0]
          aliases = self.alias_db.get_aliases(channel, name)
          if aliases:
            sep = ", "
            if len(aliases) > 2:
              aliases[-1] = "and %s" % (aliases[-1])
            elif len(aliases) == 2:
              sep = " and "
            irc.reply("%s is known as %s." % (name, sep.join(aliases)))
          else:
            irc.reply("%s doesn't have any aliases!" % (name))
        else:
          irc.reply("Give me *something*!  A nick, a word, anything!")
    showaliases = wrap(showaliases, ['channel', any('something')])

    def karma(self, irc, msg, args, channel, things):
        """[<channel>] [<thing> ...]

        Returns the karma of <thing>.  If <thing> is not given, returns the top
        N karmas, where N is determined by the config variable
        supybot.plugins.Karma.rankingDisplay.  If one <thing> is given, returns
        the details of its karma; if more than one <thing> is given, returns
        the total karma of each of the the things. <channel> is only necessary
        if the message isn't sent on the channel itself.
        """
        if len(things) == 1:
            name = things[0]
            t = self.db.get(channel, name)
            if t is None:
                irc.reply(format('%s has neutral karma.', name))
            else:
                (added, subtracted) = t
                total = added - subtracted
                if self.registryValue('simpleOutput', channel):
                    s = format('%s: %i', name, total)
                else:
                    s = format('Karma for %q has been increased %n and '
                               'decreased %n for a total karma of %s.',
                               name, (added, 'time'), (subtracted, 'time'),
                               total)
                irc.reply(s)
        elif len(things) > 1:
            (L, neutrals) = self.db.gets(channel, things)
            if L:
                s = format('%L', [format('%s: %i', *t) for t in L])
                if neutrals:
                    neutral = format('.  %L %h neutral karma',
                                     neutrals, len(neutrals))
                    s += neutral
                irc.reply(s + '.')
            else:
                irc.reply('I didn\'t know the karma for any of those things.')
        else: # No name was given.  Return the top/bottom N karmas.
            limit = self.registryValue('rankingDisplay', channel)
            top = self.db.top(channel, limit)
            highest = [format('%q (%s)', s, t)
                       for (s, t) in self.db.top(channel, limit)]
            lowest = [format('%q (%s)', s, t)
                      for (s, t) in self.db.bottom(channel, limit)]
            if not (highest and lowest):
                irc.error('I have no karma for this channel.')
                return
            rank = self.db.rank(channel, msg.nick)
            if rank is not None:
                total = self.db.size(channel)
                rankS = format('  You (%s) are ranked %i out of %i.',
                               msg.nick, rank, total)
            else:
                rankS = ''
            s = format('Highest karma: %L.  Lowest karma: %L.%s',
                       highest, lowest, rankS)
            irc.reply(s, prefixNick=False)
    karma = wrap(karma, ['channel', any('something')])

    _mostAbbrev = utils.abbrev(['increased', 'decreased', 'active'])
    def most(self, irc, msg, args, channel, kind):
        """[<channel>] {increased,decreased,active}

        Returns the most increased, the most decreased, or the most active
        (the sum of increased and decreased) karma things.  <channel> is only
        necessary if the message isn't sent in the channel itself.
        """
        L = self.db.most(channel, kind,
                         self.registryValue('mostDisplay', channel))
        if L:
            L = [format('%q: %i', name, i) for (name, i) in L]
            irc.reply(format('%L', L))
        else:
            irc.error('I have no karma for this channel.')
    most = wrap(most, ['channel',
                       ('literal', ['increased', 'decreased', 'active'])])

    def clear(self, irc, msg, args, channel, name):
        """[<channel>] <name>

        Resets the karma of <name> to 0.
        """
        self.db.clear(channel, name)
        irc.replySuccess()
    clear = wrap(clear, [('checkChannelCapability', 'op'), 'text'])

    def dump(self, irc, msg, args, channel, filename):
        """[<channel>] <filename>

        Dumps the Karma database for <channel> to <filename> in the bot's
        data directory.  <channel> is only necessary if the message isn't sent
        in the channel itself.
        """
        self.db.dump(channel, filename)
        irc.replySuccess()
    dump = wrap(dump, [('checkCapability', 'owner'), 'channeldb', 'filename'])

    def load(self, irc, msg, args, channel, filename):
        """[<channel>] <filename>

        Loads the Karma database for <channel> from <filename> in the bot's
        data directory.  <channel> is only necessary if the message isn't sent
        in the channel itself.
        """
        self.db.load(channel, filename)
        irc.replySuccess()
    load = wrap(load, [('checkCapability', 'owner'), 'channeldb', 'filename'])

Class = NewKarma

# vim:set shiftwidth=4 softtabstop=4 expandtab textwidth=79:
