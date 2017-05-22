asck - ArchivesSpace Data Checker plugin
========================================

This plugin introduces a new background job that checks the data in an ArchivesSpace instance.
It reports on the number of records found and how many are invalid or throw errors.

## Getting Started

Enable the plugin by editing the file in `config/config.rb`:

    AppConfig[:plugins] = ['some_plugin', 'asck']


## What does it do?

It goes through all ASModels that have a corresponding JSONModel and
attempts to validate each record it finds.


