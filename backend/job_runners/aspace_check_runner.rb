require 'jsonmodel'

class ASpaceCheckRunner < JobRunner

  register_for_job_type('aspace_check_job', :allow_reregister => true)
#  register_for_job_type('aspace_check_job', :create_permissions => :manage_repository,
#                                            :cancel_permissions => :manage_repository)

  def run
    # Some models are repo scoped but don't have a repo_id.
    # At the time of writing, these:
    #   CollectionManagement, Deaccession, RdeTemplate,
    #   RevisionStatement, UserDefined
    #
    # This requires a little hoop jumping, like:
    #   - giving a repo context to the global check
    #   - checking if a model has a repo_id column
    #
    # This is annoying, but what are you going to do?

    @models = []
    @total_count = 0
    @invalid_count = 0
    @no_index_count = 0
    @error_count = 0

    @start_time = Time.now

    log("--------------------------")
    log("ArchivesSpace Data Checker")
    log("--------------------------")
    log("Started at: " + @start_time.to_s)
    log("Database: " + AppConfig[:db_url].sub(/\?.*/, ''))
    log("Skipping validations") if @json.job['skip_validation']
    if @json.job['models'].empty?
      log("Checking all models")
      @models = ASModel.all_models
    else
      log("Checking models: " + @json.job['models'].join(' '))
      bad_models = []
      @json.job['models'].each do |name|
        begin
          model = Object.const_get(name)
          if model.included_modules.include?(ASModel)
            @models << model
          else
            bad_models << name
          end
        rescue NameError => e
          bad_models << e.to_s.split.last
        end
      end
      unless bad_models.empty?
        log_error("Some models are not ASModels: " + bad_models.join(' '))
        log_error("Aborting ...")
        return
      end
    end
    log("--")

    # globals first
    log("Global:")
    RequestContext.open(:repo_id => 0) do
      check_models(:global => true)
    end

    # then by repo
    Repository.each do |repo|
      break if self.canceled?
      log("--")
      log("Repository: #{repo.repo_code} (id=#{repo.id})")
      RequestContext.open(:repo_id => repo.id) do
        check_models
      end
    end

    log("--")
    
    if self.canceled?
      log("Check canceled! Incomplete results follow.")
    else
      log("Check complete.")
    end
    log("#{@total_count} record#{@total_count == 1 ? '' : 's'} found in #{Repository.count} repositories.")

    if @json.job['skip_validation']
      log("Records were not validated.")
    else
      log("#{@invalid_count} record#{@invalid_count == 1 ? '' : 's'} are invalid.")
      log("#{@no_index_count} record#{@no_index_count == 1 ? '' : 's'} were not found in the search index.")
      log("#{@error_count} record#{@error_count == 1 ? '' : 's'} errored.")
    end

    log("--")
    @end_time = Time.now
    log("Started at:   " + @start_time.to_s)
    log("Ended at:     " + @end_time.to_s)
    log("Elapsed time: #{(@end_time - @start_time + 0.5).to_i}s")

    self.success! unless self.canceled?
  end


  def check_models(opts = {})
    global = opts.fetch(:global, false)

    @models.each do |model|
      break if self.canceled?

      next unless model.has_jsonmodel?
      next unless model.model_scope(true)

      # some models declare themselves as repo scoped, but don't have repo_ids
      # treat them as globals
      next if (model.model_scope == :global || !model.columns.include?(:repo_id)) != global

      if global
        check_records(model)
      else
        check_records(model, model.where(:repo_id => model.active_repository))
      end
    end
  end


  def check_records(model, ds = nil)
    ds ||= model
    @model_count = ds.count
    @total_count += @model_count

    if @json.job['skip_validation']
      log("#{model}: #{@model_count}") if @model_count > 0
    else
      no_index_for_model = 0
      ds.each do |record|
        break if self.canceled?
        begin
          json = model.to_jsonmodel(record[:id])

          if index_for_model(model, json) && json.uri
            records = Search.records_for_uris(Array(json.uri))
            if records['total_hits'] == 0
              @no_index_count += 1
              no_index_for_model += 1
              if no_index_for_model <= 5
                log_error("Record not found in index: #{json.uri}")
              elsif no_index_for_model == 6
                log_error("... more unindexed records not shown")
              end
            end
          end

        rescue JSONModel::ValidationException => e
          @invalid_count += 1
          log_error("Invalid record: #{model} #{record[:id]} -- #{e}")
        rescue => e
          @error_count += 1
          log_error("Record errored: #{model} #{record[:id]} -- #{e}", e)
        end
      end
    end
  end


  def index_for_model(model, json)
    @index_for_model ||= {}

    type = json.class.record_type
    key = "#{model.active_repository}__#{type}"

    unless @index_for_model.has_key?(key)
      results = Search.search({:type => type, :page => 1, :page_size => 1}, model.active_repository)
      @index_for_model[key] = results['total_hits'] > 0 ? results['total_hits'] : false
      if @index_for_model[key]
        log("#{model}: #{@model_count} #")
        if @index_for_model[key] != @model_count
          log_error("Index count incorrect: #{@index_for_model[key]}")
        end
      else
        log("#{model}: #{@model_count}")
      end
    end

    @index_for_model[key]
  end


  def log(s)
    Log.debug(s)
    @job.write_output(s)
  end


  def log_error(s, e = nil)
    log("  *** #{s}")
    Log.debug(e.backtrace.join("\n")) if e
  end
end
