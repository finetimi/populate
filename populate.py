# stdlib imports
import datetime
import logging
from importlib import import_module
from typing.re import Pattern

# django imports
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q

# local imports
from newsfeed.populate import populate_source
from newsfeed.models import SourceParent
import crawler.source

logger = logging.getLogger('commands')
TODAY = datetime.date.today()


class Command(BaseCommand):
	help = u"Populate Newsfeed models with predifined data"

	commands_dict = {
		"parent": "populate_parent",
		"source": "sources",
	}

	def add_arguments(self, parser):

		parser.add_argument('-u', '--url', type=str, help='Takes a url, creates a '
			                         'Source or Parent object from the url, and populates '
			                         'the database with the source', nargs='+')
		parser.add_argument('populate_type', nargs=1, type=str, choices=['source', 'parent'])
		parser.add_argument('module', nargs='+', type=str)

	def handle(self, *args, **options):
		"""
		Checks the value of the first argument passed in against
		commands dict, if the command exists, get the method from
		this object and call it.
		"""
		self.populate_type = options['populate_type'][0]

		if options.get('url'):
			return self.populate_from_url(options.get('url'))

		# set required modules
		if 'all' in options['module']:
			self.modules = crawler.source.god_dict.values()
		else:
			self.modules = []
			for module in options['module']:
				file = getattr(crawler.source, module)
				self.modules.append(file.Source)

		return getattr(self, self.commands_dict[self.populate_type])()

		raise CommandError('Please pass in a populate command or flag')

	def populate_parent(self):
		"""
		Grab all SourceParents from crawler.source.__init__.py.god_dict,
		iterate through the dictionary and populate the database with the
		properties of each SourceParent in god_dict. For this to work each
		SourceParent object has to be initialized with a url, so use localhost
		then access its props and populate the db with the props.
		Each SourceParent's identifier is unique and can be used to create or update
		the SourceParent model in the db
		"""

		self.stdout.write(self.style.SUCCESS(f'Parsing {len(self.modules)} source parents'))
		for SourceObj in self.modules:
			try:
				# initialize Parent with localhost to get access to it's properties
				parent_obj = SourceObj('http://localhost:8000')
				url = parent_obj.props.get('parent_url')
				name = parent_obj.props.get('parent_name')
				created = False

				'''
				Convert all regex values to keys
				'''
				for k, v in parent_obj.props.items():
					if isinstance(v, Pattern):
						parent_obj.props[k] = v.pattern

				self.stdout.write(self.style.SUCCESS(f'Populating {name} parent object...'))
				with transaction.atomic():
					try:
						obj = SourceParent.objects.get(url=url)
						if parent_obj.props != obj.latest_props:
							obj.properties[str(TODAY)] = parent_obj.props
							obj.name = name
							obj.save()
							self.stdout.write(self.style.SUCCESS(f'SourceParent {name} already exists >> Updating...'))
						else:
							self.stdout.write(self.style.SUCCESS(f'SourceParent {name} already exists skipping update...'))
					except SourceParent.DoesNotExist:
						obj = SourceParent.objects.create(
							name=name,
							url=url,
							properties={str(TODAY): parent_obj.props}
						)
						created = True
				if created:
					self.stdout.write(self.style.SUCCESS('%s created successfully...' % obj.name))
					# check for a previous version of source name
					if SourceParent.objects.filter(Q(name=obj.name) | Q(url=obj.url)).count() > 1:
						self.stdout.write(self.style.WARNING('Noticed there are more than one '
	                                            'parent objects with name {},'
	                                            ' ensure no duplicates'.format(obj.name)))
			except Exception as e:
				logger.exception(self.style.ERROR(f'{name} raised >> {str(e)}'))

	def sources(self):
		"""
		Populate the database with Source objects for each Parent
		in crawler.source.god_dict
		DB population can be done by calling the build_sources method
		for each Source then populating the db with the list of source
		objects returned from build_sources.
		"""
		for SourceObj in self.modules:
			parent_obj = SourceObj('http://localhost:8000')
			parent_name = parent_obj.props.get('parent_name')
			parent_url = parent_obj.props.get("parent_url")

			try:
				self.stdout.write(self.style.SUCCESS(f'Initializing {parent_name} >> {parent_url} ...'))
				self.load_source(SourceObj)
			except Exception as e:
				logger.exception(
					self.style.ERROR(
						f'An exception occured while building '
						f'sources for {parent_name} >> {parent_url}'
						f'>>> {str(e)}'
					)
				)
				continue

	def load_source(self, source_obj):
		from newsfeed.models import Source
		"""
		Populates the database with sources from the module provided.
		Import module from crawler.source and build_sources from the
		modules Source object.
		:params source_obj: the source object imported from the module
		"""

		sources = source_obj.build_sources()
		created = [populate_source(source) for source in sources if source]

		for obj in created:
			count = Source.objects.filter(Q(name__iexact=obj.name) | Q(homepage=obj.homepage)).count()
			if count > 1:
				self.stdout.write(self.style.WARNING('Noticed there are {} '
                                         'source objects with name {} or homepage {},'
                                         ' ensure no duplicates'.format(count, obj.name, obj.homepage)))

	def populate_from_url(self, urls):
		from crawler.source import Source
		for url in urls:
			obj = Source(url)
			obj.parse()
			populate_source(obj)
			return
