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

    @total_count = 0
    @invalid_count = 0
    @error_count = 0

    @start_time = Time.now

    log("--------------------------")
    log("ArchivesSpace Data Checker")
    log("--------------------------")
    log("Started at: " + @start_time.to_s)
    log("Database: " + AppConfig[:db_url].sub(/\?.*/, ''))
    log("Skipping validations") if @json.job['skip_validation']
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

    ASModel.all_models.each do |model|
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

    @total_count += ds.count
    log("#{model}: #{ds.count}")

    unless @json.job['skip_validation']
      ds.each do |record|
        break if self.canceled?
        begin
          json = model.to_jsonmodel(record[:id])
        rescue JSONModel::ValidationException => e
          @invalid_count += 1
          log("  * Invalid record: #{model} #{record[:id]} -- #{e}")
        rescue => e
          @error_count += 1
          log("  * Record errored: #{model} #{record[:id]} -- #{e}")
        end
      end
    end
  end


  def log(s)
    Log.debug(s)
    @job.write_output(s)
  end

end
