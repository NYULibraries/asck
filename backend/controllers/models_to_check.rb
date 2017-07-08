 class ArchivesSpaceService < Sinatra::Base
   Endpoint.get('/models_to_check')
    .description("List all models available to be checked by asck")
    .params()
    .permissions([])
    .returns([200, "A list of models"]) \
  do
     json_response(ASModel.all_models.select{|m| m.has_jsonmodel?}.map(&:name).sort)
  end
end
