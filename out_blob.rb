#!/usr/bin/ruby -w
require 'fluent/output'
require 'fluent/env'
require 'fluent/config/error'
require 'azure/storage/blob'
require 'openssl'
require 'securerandom'
require 'rbconfig'
require 'json'
require 'json/ext'
require 'fluent/plugin/buffer'
require 'time'

module Fluent::Plugin

	class Out_Blob< Fluent::Plugin::Output
		
		#to register the custom plugin as a new valid fluentd plugin
		Fluent::Plugin.register_output('blob',self)

		#config parameters

		desc 'resource ID'
		config_param :resourceId, :string

		desc 'deployment ID'
		config_param :DeploymentId, :string

		desc 'host'
		config_param :Host, :string

		desc 'the timestamp to use for records sent to blobs'
		config_param :use_source_timestamp, :bool, :default => true

		desc 'the field name for the event emit time stamp'
		config_param :emit_timestamp_name, :string, :default => "FluentdIngestTimestamp"

		desc 'storage account name'
		config_param :account_name, :string

		desc 'SAS token for storage account'
		config_param :sas_token, :string
		
		desc 'name of container'
		config_param :container, :string

		desc 'identity hash'
		config_param :identity_hash, :string
		
		#check if the container already exists
		def check_container(blob_client)

			begin
				if(blob_client.get_container_properties(@container))
					return true
				end
			rescue
				return false
			end
		end
	
		def check_blob(blob_client,blob_name)

			begin
				if(blob_client.get_blob_properties(@container,blob_name))
					return true
				end				
			rescue 
				return false
			end

		end
		 
		#get blob name for the instant
		def get_blob_name

			name=""
			name+="resourceId=" + @resourceId + "/"
			name+="i=" + @identity_hash + "/"
			
			time=Time.now

			name+="y=" + time.strftime("%Y").to_s + "/"
			name+="m=" + time.strftime("%m").to_s + "/"
			name+="d=" + time.strftime("%d").to_s + "/"
			name+="h=" + time.strftime("%H").to_s + "/"
			name+="m=" + "00" + "/"
			name+="PT1H.json"

		end
				
		def get_last_blob(blob_client)

			nextMarker = nil
			last_name=""
			last=""

			loop do
				blobs = blob_client.list_blobs(@container, { marker: nextMarker })
				blobs.each do |blob|
					last=last_name
					last_name= blob.name
				end
				nextMarker = blobs.continuation_token
				break unless nextMarker && !nextMarker.empty?
			end

			return last
		end

		#splits a property value pair
		def split_msg(msg)
				
			property,value=msg.split("=>")
			content="		" + property + " : " + value
			return content

		end

		def get_operation(tag)
		
			system,operationName,category,level = tag.split(".")
			return operationName

		end	

		#formats the syslog record received by the output plugin
		def format_syslog_msg(msg)
			
			cont=""
			i=1
				
			while i<10;

				first,rest=msg.split(", ",2)
				
				if i!=2
					cont+=split_msg(first) + ",\n"
				end		
				
				msg=rest
				i+=1

			end

			first,rest=msg.split("}",2)
			cont+=split_msg(first) + ",\n"

			return cont
		end

		#formats the file record received by the output plugin
		def format_file_msg(msg)

			cont=""
			first,second=msg.split(", ")
			cont+=split_msg(first) + ",\n"
			cont+=split_msg(second) + ",\n"
			return cont

		end


		#initializes the plugin 
		def initialize

			super
			#for blob 
			$stdout.sync = true	
			@created_blob=false
		end


		def formatted_to_msgpack_binary 
			return true
		end

		#to specify the format in which record is received
		def format (tag,time,record)

			if use_source_timestamp
				[tag,time,record].to_msgpack
			else
				[tag, record].to_msgpack
			end

		end
	
		def start

			super

		end

		def shutdown 

			super

		end

		#formatting the blob content before the record
		def pre_text
			
			content=""

			if @created_blob == true
				content+="{\"records\":[\n"
				@created_blob=false
			else
				content+=",\n"
			end
		
			time= Time.now.strftime('%Y-%m-%dT%H:%M:%SZ').to_s
			content+="{	\"time\" : \"" + time + "\",\n"
			content+="	\"resourceId\" : \"" + @resourceId + "\",\n"
			content+="	\"properties\" : {\n"
			return content

		end

		#formatting the blob content after the record
		def post_text

			content="		\"DeploymentId\" : \"" + @DeploymentId + "\",\n"
			content+="		\"Host\" : \"" + @Host + "\"\n"
			content+="	},\n"
			return content

		end

		#formatting the tag		
		def tag_text(tag)
			
			system,operationName,category,level = tag.split(".")

			if(operationName=="file")
				category="Unknown"
				level="Unknown"
			end
						
			content="	\"category\" : \"" + category + "\",\n"
			content+="	\"level\" : \"" + level + "\",\n"
			content+=" 	\"operationName\" : \""

			sysOperation=system+operationName
			
			if sysOperation == "systemsyslog"
				content+="LinuxSyslogEvent"
			else
				content+= "Unknown"
			end

			content+="\"\n}"
		
			return content

		end

		#format the blob 
		def handle_record(record,tag)
			
			content=pre_text
			
			operation=get_operation(tag)

			msg=record.to_s

			#check if it is file or syslog
			if operation == "syslog"
				first,rest=msg.split("{",2)
				content+=format_syslog_msg(rest)
			else
				content+=format_file_msg(msg)
			end

			content+=post_text

			content+=tag_text(tag)

			return content
		end
		
		#for asynchronous buffered output mode
		def try_write(chunk)
			
			#blob client to access the blobs in the storage account using SAS token
			@blob_client = Azure::Storage::Blob::BlobService.create(storage_account_name:@account_name, storage_sas_token: @sas_token)
			

			if(!check_container(@blob_client))
				
				container = @blob_client.create_container(@container,options={})
				@blob_client.set_container_acl(@container, "container")

			end
			
			blob_name=get_blob_name

			#create an append blob if one does not already exist
			if (!check_blob(@blob_client,blob_name))
			
				@blob_client.create_append_blob(@container,blob_name)
				@created_blob=true

				@last_blob=get_last_blob(@blob_client)
				
				if (@last_blob!="")	
					@blob_client.append_blob_block(@container, @last_blob, "\n]}")
				end

			end

			chunk_id = chunk.unique_id
			content=""

			if use_source_timestamp

					chunk.msgpack_each {  |(tag, time, record)|
		
						record[emit_timestamp_name] = Time.now.strftime('%Y-%m-%dT%H:%M:%SZ')
						content+=handle_record(record,tag)
					
					}
			else

				chunk.msgpack_each {|(tag, record)|
				
					record[emit_timestamp_name] = Time.now.strftime('%Y-%m-%dT%H:%M:%SZ')
					content+=handle_record(record,tag)
				
				}

			end	
			
			@log.flush		
			
			commit_write(chunk_id)

			#append to an existing blob
			@blob_client.append_blob_block(@container, blob_name, content)

		end#method try_write			
	end # class Out_Blob
end #module Fluent::Plugin
