import json
import requests
from requests_aws4auth import AWS4Auth
import boto3




CLUSTER_ENDPOINTS = {
    "cluster1": "https://your-cluster-domain.region.es.amazonaws.com",
    "cluster2": "https://your-cluster-domain.region.es.amazonaws.com"
    # Add more clusters as needed
}


region = "us-east-1"  # Update to your AWS region
service = "es"




credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)




def get_cluster_health(domain_endpoint):
   url = f"{domain_endpoint}/_cluster/health"
   response = requests.get(url, auth=awsauth)
   return response.json()




def get_indices(domain_endpoint):
   url = f"{domain_endpoint}/_cat/indices?format=json"
   response = requests.get(url, auth=awsauth)
   return response.json()




def get_node_stats(domain_endpoint):
   url = f"{domain_endpoint}/_nodes/stats/fs"
   response = requests.get(url, auth=awsauth)
   return response.json()




def get_node_jvm_stats(domain_endpoint):
   """Get JVM and CPU statistics from OpenSearch nodes"""
   url = f"{domain_endpoint}/_nodes/stats/jvm,os"
   response = requests.get(url, auth=awsauth)
   return response.json()




def analyze_jvm_cpu_metrics(domain_endpoint):
   """Analyze JVM heap usage and CPU metrics"""
   stats = get_node_jvm_stats(domain_endpoint)
   high_usage_nodes = []
   all_node_metrics = []
  
   for node_id, data in stats["nodes"].items():
       # JVM Heap metrics
       jvm = data.get("jvm", {})
       heap = jvm.get("mem", {}).get("heap_used_percent", 0)
      
       # CPU metrics (if available)
       os_data = data.get("os", {})
       cpu_percent = os_data.get("cpu", {}).get("percent", None)
      
       # GC metrics
       gc = jvm.get("gc", {}).get("collectors", {})
       old_gen_gc = gc.get("old", {})
       young_gen_gc = gc.get("young", {})
      
       node_metrics = {
           "node_name": data.get("name", "unknown"),
           "heap_used_percent": heap,
           "cpu_percent": cpu_percent,
           "gc_old_collection_count": old_gen_gc.get("collection_count", 0),
           "gc_young_collection_count": young_gen_gc.get("collection_count", 0),
           "gc_old_time_ms": old_gen_gc.get("collection_time_in_millis", 0),
           "gc_young_time_ms": young_gen_gc.get("collection_time_in_millis", 0)
       }
      
       all_node_metrics.append(node_metrics)
      
       # Flag nodes with high resource usage
       if heap > 85 or (cpu_percent and cpu_percent > 90):
           high_usage_nodes.append(node_metrics)
  
   return high_usage_nodes, all_node_metrics




def analyze_disk_space(domain_endpoint):
   stats = get_node_stats(domain_endpoint)
   low_disk_nodes = []
   all_nodes_info = []
  
   for node_id, data in stats["nodes"].items():
       total = data["fs"]["total"]["total_in_bytes"]
       free = data["fs"]["total"]["available_in_bytes"]
       percent_free = (free / total) * 100
      
       node_info = {
           "node_name": data["name"],
           "percent_free": round(percent_free, 2),
           "free_gb": round(free / (1024**3), 2),
           "total_gb": round(total / (1024**3), 2)
       }
      
       all_nodes_info.append(node_info)
      
       if percent_free < 15:
           low_disk_nodes.append(node_info)
  
   return low_disk_nodes, all_nodes_info




def get_user_response(event):
   """Extract user's response from different possible input sources"""
   # Get from inputTranscript (user's actual message)
   user_input = event.get("inputTranscript", "").lower().strip()
  
   # Also check slots for structured responses
   if "sessionState" in event:
       slots = event.get("sessionState", {}).get("intent", {}).get("slots", {})
       if "UserResponse" in slots and slots["UserResponse"]:
           slot_value = slots["UserResponse"].get("value", {})
           if isinstance(slot_value, dict):
               slot_response = slot_value.get("interpretedValue", "").lower().strip()
               if slot_response:  # Use slot value if available
                   user_input = slot_response
  
   print(f"DEBUG - Raw user_input extracted: '{user_input}'")
   return user_input




def get_cluster_name(event):
   """Extract cluster name from event"""
   cluster_name = None
  
   print(f"DEBUG - get_cluster_name input event keys: {list(event.keys())}")
  
   if "sessionState" in event:
       # Check session attributes first (from previous conversation)
       session_attrs = event.get("sessionState", {}).get("sessionAttributes", {})
       print(f"DEBUG - Session attributes: {session_attrs}")
      
       if "cluster_name" in session_attrs:
           print(f"DEBUG - Found cluster_name in session: {session_attrs['cluster_name']}")
           return session_attrs["cluster_name"]
      
       # Then check slots
       slots = event.get("sessionState", {}).get("intent", {}).get("slots", {})
       print(f"DEBUG - All slots: {slots}")
      
       if "ClusterName" in slots and slots["ClusterName"]:
           cluster_name_obj = slots["ClusterName"].get("value", {})
           print(f"DEBUG - ClusterName slot object: {cluster_name_obj}")
          
           if isinstance(cluster_name_obj, dict):
               cluster_name = cluster_name_obj.get("interpretedValue")
               print(f"DEBUG - Extracted from interpretedValue: {cluster_name}")
           else:
               cluster_name = cluster_name_obj
               print(f"DEBUG - Used direct value: {cluster_name}")
  
   print(f"DEBUG - Final cluster_name result: {cluster_name}")
   return cluster_name




def handle_initial_request(cluster_name, domain_endpoint):
   """Handle the initial cluster health check request"""
   health = get_cluster_health(domain_endpoint)
   cluster_status = health.get("status", "unknown").upper()
   node_count = health.get("number_of_nodes", 0)
   unassigned_shards = health.get("unassigned_shards", 0)
  
   # Initial status message
   if cluster_status == "GREEN":
       message = f"✅ Fetching status for cluster '{cluster_name}': Diagnosis = GREEN\n\nYour cluster is healthy! All shards are properly allocated. No troubleshooting needed."
       return {
           "message": message,
           "next_step": "complete",
           "session_data": {"cluster_name": cluster_name, "status": cluster_status}
       }
  
   elif cluster_status == "RED":
       message = f"🔴 Fetching status for cluster '{cluster_name}': Diagnosis = RED\n\n⚠️ CRITICAL: Your cluster has missing PRIMARY shards - potential data loss!\n\nWould you like me to walk you through emergency troubleshooting? (Y/N)"
       return {
           "message": message,
           "next_step": "red_troubleshooting_confirm",
           "session_data": {"cluster_name": cluster_name, "status": cluster_status}
       }
  
   else:  # YELLOW
       message = f"🟡 Fetching status for cluster '{cluster_name}': Diagnosis = YELLOW\n\nYour cluster has {unassigned_shards} unassigned shards. This means your data is safe, but some replica shards aren't allocated.\n\nWould you like me to walk you through troubleshooting? (Y/N)"
       return {
           "message": message,
           "next_step": "yellow_troubleshooting_confirm",
           "session_data": {"cluster_name": cluster_name, "status": cluster_status, "node_count": node_count}
       }




def handle_troubleshooting_steps(step, user_response, session_data, domain_endpoint):
   """Handle the step-by-step troubleshooting process"""
   cluster_name = session_data["cluster_name"]
  
   if step == "yellow_troubleshooting_confirm":
       print(f"DEBUG - In yellow_troubleshooting_confirm, user_response: '{user_response}'")
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           return {
               "message": "Great! For the first step of troubleshooting, I need to check if this is a single-node cluster.\n\nWould you like me to proceed? (Y/N)",
               "next_step": "check_single_node",
               "session_data": session_data
           }
       else:
           print(f"DEBUG - User declined troubleshooting with response: '{user_response}'")
           return {
               "message": "No problem! If you need troubleshooting help later, just ask me to check your cluster again.",
               "next_step": "complete",
               "session_data": session_data
           }
  
   elif step == "check_single_node":
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           node_count = session_data.get("node_count", 0)
          
           if node_count == 1:
               message = """✅ Single-node cluster detected!




Single-node clusters always show YELLOW status because replicas cannot be assigned (there's nowhere else to put them).




To achieve GREEN status:
- Increase your node count to 2+ nodes, OR
- Set replica count to 0 for single-node setups




This is normal behavior for single-node clusters."""
               return {
                   "message": message,
                   "next_step": "complete",
                   "session_data": session_data
               }
           else:
               message = f"✅ Not a single-node cluster (you have {node_count} nodes).\n\nLet's move to step 2: checking disk space on your nodes.\n\nWould you like me to proceed? (Y/N)"
               return {
                   "message": message,
                   "next_step": "check_disk_space",
                   "session_data": session_data
               }
       else:
           return {
               "message": "No problem! Feel free to ask if you need help later.",
               "next_step": "complete",
               "session_data": session_data
           }
  
   elif step == "check_disk_space":
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           low_disk_nodes, all_nodes_info = analyze_disk_space(domain_endpoint)
          
           if low_disk_nodes:
               problem_nodes = []
               for node in low_disk_nodes:
                   problem_nodes.append(f"  • {node['node_name']}: {node['percent_free']}% free")
               nodes_text = "\n".join(problem_nodes)
              
               message = f"""🔴 Low disk space detected!




Nodes with low disk space (<15% free):
{nodes_text}




Why this causes YELLOW status: OpenSearch stops allocating shards when nodes are low on space.




Try these solutions:
1. Delete any unwanted indices
2. Scale up the EBS volume
3. Add more data nodes




This is likely the cause of your yellow cluster status."""
              
               return {
                   "message": message,
                   "next_step": "complete",
                   "session_data": session_data
               }
           else:
               avg_free = sum(node["percent_free"] for node in all_nodes_info) / len(all_nodes_info)
               message = f"✅ Disk space looks good (average {avg_free:.1f}% free across nodes).\n\nLet's move to step 3: checking for high JVM/CPU usage.\n\nWould you like me to proceed? (Y/N)"
               return {
                   "message": message,
                   "next_step": "check_jvm_cpu",
                   "session_data": session_data
               }
       else:
           return {
               "message": "No problem! Feel free to ask if you need help later.",
               "next_step": "complete",
               "session_data": session_data
           }
  
   elif step == "check_jvm_cpu":
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           high_usage_nodes, all_node_metrics = analyze_jvm_cpu_metrics(domain_endpoint)
          
           if high_usage_nodes:
               problem_nodes = []
               for node in high_usage_nodes:
                   cpu_text = f"{node['cpu_percent']}%" if node['cpu_percent'] else "N/A"
                   problem_nodes.append(f"  • {node['node_name']}: {node['heap_used_percent']}% heap, {cpu_text} CPU")
               nodes_text = "\n".join(problem_nodes)
              
               message = f"""🔴 High JVM/CPU usage detected!




Nodes with high resource usage:
{nodes_text}




Why this causes YELLOW status: High JVM heap or CPU usage can prevent proper shard allocation.




Solutions:
 - Scale up your instances (more CPU/memory)
 - Reduce query load temporarily
 - Check for inefficient queries or indexing
 - Consider adding more nodes to distribute load




This high resource usage is likely causing your yellow cluster status."""
              
               return {
                   "message": message,
                   "next_step": "complete",
                   "session_data": session_data
               }
           else:
               # Calculate averages
               avg_heap = sum(node["heap_used_percent"] for node in all_node_metrics) / len(all_node_metrics)
               cpu_nodes = [node for node in all_node_metrics if node["cpu_percent"] is not None]
               avg_cpu = sum(node["cpu_percent"] for node in cpu_nodes) / len(cpu_nodes) if cpu_nodes else None
              
               cpu_text = f", {avg_cpu:.1f}% CPU" if avg_cpu else ""
              
               message = f"✅ JVM/CPU levels appear normal (average {avg_heap:.1f}% heap{cpu_text}).\n\nLet's move to step 4: checking replica configuration.\n\nWould you like me to proceed? (Y/N)"
               return {
                   "message": message,
                   "next_step": "check_replica_config",
                   "session_data": session_data
               }
       else:
           return {
               "message": "No problem! Feel free to ask if you need help later.",
               "next_step": "complete",
               "session_data": session_data
           }
  
   elif step == "check_replica_config":
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           indices = get_indices(domain_endpoint)
           node_count = session_data.get("node_count", 0)
          
           # Fix: Ensure we only process valid integer replica counts
           replica_counts = []
           for idx in indices:
               if "rep" in idx and idx["rep"]:
                   try:
                       rep_count = int(idx["rep"])
                       replica_counts.append(rep_count)
                   except (ValueError, TypeError):
                       # Skip invalid replica values
                       continue
          
           max_replica_count = max(replica_counts) if replica_counts else 0
          
           if max_replica_count >= int(node_count):
               message = f"""⚙️ Replica configuration issue found!




Problem: You have indices with {max_replica_count} replicas, but only {node_count} nodes.




Why this causes YELLOW: Replicas can't be allocated because there aren't enough nodes to place them on.




Solutions:
1. Reduce replica count: PUT _all/_settings {{"index":{{"number_of_replicas":1}}}}
2. Add more nodes to accommodate current replica settings




This is likely causing your yellow cluster status."""
              
               return {
                   "message": message,
                   "next_step": "complete",
                   "session_data": session_data
               }
           else:
               message = f"✅ Replica configuration looks reasonable (max {max_replica_count} replicas for {node_count} nodes).\n\nLet's move to step 5: checking for node failures.\n\nWould you like me to proceed? (Y/N)"
               return {
                   "message": message,
                   "next_step": "check_node_failures",
                   "session_data": session_data
               }
       else:
           return {
               "message": "No problem! Feel free to ask if you need help later.",
               "next_step": "complete",
               "session_data": session_data
           }
  
   elif step == "check_node_failures":
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           # Check if all expected nodes are present and healthy
           health = get_cluster_health(domain_endpoint)
           node_count = health.get("number_of_nodes", 0)
           expected_nodes = session_data.get("node_count", node_count)
          
           if int(node_count) < int(expected_nodes):
               message = f"""🔴 Node failure detected!




Expected nodes: {expected_nodes}
Current nodes: {node_count}




Some nodes appear to have failed or disconnected. This is likely causing your yellow status.




Solutions:
 - Check CloudWatch for node health metrics
 - Verify if nodes crashed or were terminated
 - Restart failed nodes if needed
 - Check network connectivity between nodes




This node failure is likely the cause of your yellow cluster status."""
              
               return {
                   "message": message,
                   "next_step": "complete",
                   "session_data": session_data
               }
           else:
               message = f"✅ All {node_count} nodes appear healthy and connected.\n\nLet's move to step 6: checking for newly created indices.\n\nWould you like me to proceed? (Y/N)"
               return {
                   "message": message,
                   "next_step": "check_newly_created_index",
                   "session_data": session_data
               }
       else:
           return {
               "message": "No problem! Feel free to ask if you need help later.",
               "next_step": "complete",
               "session_data": session_data
           }
  
   elif step == "check_newly_created_index":
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           # Check for recently created indices by looking at creation times
           indices = get_indices(domain_endpoint)
           recent_indices = []
          
           # Note: This is a simplified check. In practice, you'd want to check index creation timestamps
           # For now, we'll ask the user directly about recent index creation
           message = """Have you recently created any new indices in the last few hours?




If YES: Multi-node clusters might briefly show YELLOW status after creating new indices. This is normal behavior as OpenSearch replicates data across the cluster.




Solution: This status typically self-resolves within minutes as OpenSearch completes the replication process.




Did you create any new indices recently? (Y/N)"""
          
           return {
               "message": message,
               "next_step": "confirm_new_index_creation",
               "session_data": session_data
           }
       else:
           return {
               "message": "No problem! Feel free to ask if you need help later.",
               "next_step": "complete",
               "session_data": session_data
           }
  
   elif step == "confirm_new_index_creation":
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           message = """✅ Recent index creation confirmed!




This explains your yellow cluster status. When new indices are created:




1. OpenSearch initially places primary shards
2. Replica shards are then allocated across other nodes 
3. During this process, the cluster shows YELLOW status
4. Once replication completes, status returns to GREEN




Solution: Wait 5-10 minutes for the replication to complete. The status should self-resolve.




Monitor with: GET _cat/indices?v to see when all shards are allocated.




This is normal behavior and not a cause for concern."""
          
           return {
               "message": message,
               "next_step": "complete",
               "session_data": session_data
           }
       else:
           message = "✅ No recent index creation.\n\nLet's move to the final step: checking for other allocation issues.\n\nWould you like me to proceed? (Y/N)"
           return {
               "message": message,
               "next_step": "check_allocation_issues",
               "session_data": session_data
           }
  
   elif step == "check_allocation_issues":
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           health = get_cluster_health(domain_endpoint)
           unassigned_shards = health.get("unassigned_shards", 0)
          
           message = f"""Final diagnosis for your yellow cluster:




Status: {unassigned_shards} unassigned shards remain after basic checks.




Likely causes:
 - Allocation awareness settings (zone/rack awareness)
 - Custom shard allocation filtering rules
 - Recent cluster changes still rebalancing




Advanced diagnostic commands:
 GET _cluster/allocation/explain
 GET _cat/shards?v&h=index,shard,prirep,state,unassigned.reason




Quick fix to try:
 POST _cluster/reroute?retry_failed=true




If these steps don't resolve the issue, consider reviewing your cluster allocation settings or contacting support."""
          
           return {
               "message": message,
               "next_step": "complete",
               "session_data": session_data
           }
       else:
           return {
               "message": "No problem! Feel free to ask if you need help later.",
               "next_step": "complete",
               "session_data": session_data
           }
  
   elif step == "red_troubleshooting_confirm":
       if user_response in ["y", "yes", "yeah", "yep", "1"]:
           message = """🚨 RED cluster emergency troubleshooting:




IMMEDIATE ACTIONS:
1. DO NOT restart nodes without understanding the cause
2. Check if any nodes crashed or were terminated
3. Verify network connectivity between nodes
4. Look for hardware/disk failures




Critical diagnostic commands:
 GET _cluster/allocation/explain
 GET _cat/nodes?v
 GET _cat/recovery?v




⚠️ WARNING: Red status means PRIMARY shards are missing - potential data loss!




Consider contacting AWS support immediately if you're unsure about data recovery procedures."""
          
           return {
               "message": message,
               "next_step": "complete",
               "session_data": session_data
           }
       else:
           return {
               "message": "Understood. Please address the RED cluster status urgently - it indicates potential data loss. Contact support if needed.",
               "next_step": "complete",
               "session_data": session_data
           }
  
   # Fallback
   return {
       "message": "I didn't understand that response. Please answer with Y (yes) or N (no).",
       "next_step": step,  # Stay on the same step
       "session_data": session_data
   }




def lambda_handler(event, context):
   try:
       print("DEBUG - Full event:", json.dumps(event, indent=2))
      
       # Get session attributes (current conversation state)
       session_attrs = event.get("sessionState", {}).get("sessionAttributes", {})
       current_step = session_attrs.get("step", "initial")
      
       # Get user input
       user_response = get_user_response(event)
       print(f"DEBUG - User response: '{user_response}'")
       print(f"DEBUG - Current step: {current_step}")
      
       if current_step == "initial":
           # First interaction - get cluster name and provide initial diagnosis
           cluster_name = get_cluster_name(event)
           print(f"DEBUG - Initial cluster_name extraction: {cluster_name}")
          
           if not cluster_name:
               # No cluster name provided - this should trigger the slot prompt in Lex
               # But if we reach here, use fallback
               cluster_name = "cluster_name"  # fallback
               print(f"DEBUG - Using fallback cluster_name: {cluster_name}")
          
           domain_endpoint = CLUSTER_ENDPOINTS.get(cluster_name)
           if not domain_endpoint:
               available_clusters = ", ".join(CLUSTER_ENDPOINTS.keys())
               return {
                   "sessionState": {
                       "dialogAction": {"type": "Close"},
                       "intent": {"name": "DiagnoseClusterIntent", "state": "Failed"}
                   },
                   "messages": [{"contentType": "PlainText", "content": f"Unknown cluster '{cluster_name}'. Available clusters: {available_clusters}"}]
               }
          
           result = handle_initial_request(cluster_name, domain_endpoint)
      
       else:
           # Continuing conversation - handle troubleshooting steps
           domain_endpoint = CLUSTER_ENDPOINTS.get(session_attrs.get("cluster_name"))
           if not domain_endpoint:
               raise Exception("Lost cluster context in session")
          
           result = handle_troubleshooting_steps(
               current_step,
               user_response,
               session_attrs,
               domain_endpoint
           )
      
       # Prepare response based on whether conversation continues or ends
       if result["next_step"] == "complete":
           # Conversation is done
           return {
               "sessionState": {
                   "dialogAction": {"type": "Close"},
                   "intent": {"name": "DiagnoseClusterIntent", "state": "Fulfilled"}
               },
               "messages": [{"contentType": "PlainText", "content": result["message"]}]
           }
       else:
           # Continue conversation - use simpler ElicitIntent without slot reference
           new_session_attrs = result["session_data"].copy()
           new_session_attrs["step"] = result["next_step"]
          
           return {
               "sessionState": {
                   "dialogAction": {"type": "ElicitIntent"},
                   "sessionAttributes": new_session_attrs,
                   "intent": {
                       "name": "DiagnoseClusterIntent",
                       "state": "InProgress"
                   }
               },
               "messages": [{"contentType": "PlainText", "content": result["message"]}]
           }
  
   except Exception as e:
       print(f"ERROR: {str(e)}")
       return {
           "sessionState": {
               "dialogAction": {"type": "Close"},
               "intent": {"name": "DiagnoseClusterIntent", "state": "Failed"}
           },
           "messages": [{"contentType": "PlainText", "content": f"Error diagnosing cluster: {str(e)}\n\nCheck OpenSearch connectivity and try again."}]
       }




