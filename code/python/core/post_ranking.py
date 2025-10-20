from core.state import NLWebHandlerState
import asyncio
from core.prompts import PromptRunner
from misc.logger.logging_config_helper import get_configured_logger
from core.schemas import Message, SenderType, MessageType

logger = get_configured_logger("post_ranking")


class PostRanking:
    """This class is used to check if any post processing is needed after the ranking is done."""
    
    def __init__(self, handler):
        self.handler = handler

    async def do(self):
        if not self.handler.connection_alive_event.is_set():
            self.handler.query_done = True
            return
        
        # Check if we should send a map message for results with addresses
        await self.check_and_send_map_message()
        
        if (self.handler.generate_mode == "none"):
            # nothing to do
            return
        
        if (self.handler.generate_mode == "summarize"):
            await SummarizeResults(self.handler).do()
            return
    
    async def check_and_send_map_message(self):
        """Check if at least half of the results have addresses and send a map message if so."""
        try:
            # Get the final ranked answers
            results = getattr(self.handler, 'final_ranked_answers', [])
            if not results:
                logger.debug("No results to check for addresses")
                return
            
            # Count results with addresses and collect map data
            results_with_addresses = []
            
            for result in results:
                # Check if result has schema_object field
                if 'schema_object' not in result:
                    logger.debug("Result missing schema_object, skipping")
                    continue
                
                schema_obj = result['schema_object']
                
                # Check for address field in schema_object
                address = None
                if isinstance(schema_obj, dict):
                    # Check for different possible address field names
                    address = (schema_obj.get('address') or 
                              schema_obj.get('location') or 
                              schema_obj.get('streetAddress') or
                              schema_obj.get('postalAddress'))
                    
                    # If address is a string, check if it looks like it has a dict representation at the end
                    if isinstance(address, str) and "{" in address:
                        # Extract just the address part before any dictionary representation
                        address = address.split(", {")[0]
                    
                    # If address is a dict, try to get a string representation
                    elif isinstance(address, dict):
                        # Handle structured address
                        address_parts = []
                        for field in ['streetAddress', 'addressLocality', 'addressRegion', 'postalCode']:
                            if field in address:
                                value = address[field]
                                # Skip if it's a dict or complex object
                                if not isinstance(value, dict):
                                    address_parts.append(str(value))
                        
                        # Handle country separately - extract just the name if it's a dict
                        if 'addressCountry' in address:
                            country = address['addressCountry']
                            if isinstance(country, dict) and 'name' in country:
                                address_parts.append(country['name'])
                            elif isinstance(country, str) and not country.startswith('{'):
                                address_parts.append(country)
                        
                        if address_parts:
                            address = ', '.join(address_parts)
                        else:
                            # If we couldn't extract parts, skip this address
                            address = None
                
                if address:
                    results_with_addresses.append({
                        'title': result.get('name', 'Unnamed'),
                        'address': str(address)
                    })
            
            # Check if at least half have addresses
            total_results = len(results)
            results_with_addr_count = len(results_with_addresses)
            
            logger.info(f"Found {results_with_addr_count} results with addresses out of {total_results} total results")
            
            if results_with_addr_count >= total_results / 2 and results_with_addr_count > 0:
                # Send the map message
                map_message = {
                    'message_type': 'results_map',
                    '@type': 'LocationMap',
                    'locations': results_with_addresses
                }
                
                logger.info(f"Sending results_map message with {results_with_addr_count} locations")
                logger.info(f"Map message content: {map_message}")
                
                try:
                    asyncio.create_task(self.handler.send_message(map_message))
                    logger.info("results_map message sent successfully")
                except Exception as e:
                    logger.error(f"Failed to send results_map message: {str(e)}", exc_info=True)
            else:
                logger.debug(f"Not sending map message - only {results_with_addr_count}/{total_results} results have addresses")
                
        except Exception as e:
            logger.error(f"Error checking/sending map message: {str(e)}")
            # Don't fail the whole post-ranking process if map generation fails
            pass
        
       
        
class SummarizeResults(PromptRunner):

    SUMMARIZE_RESULTS_PROMPT_NAME = "SummarizeResultsPrompt"

    def __init__(self, handler):
        super().__init__(handler)

    async def do(self):
        self.handler.final_ranked_answers = self.handler.final_ranked_answers[:3]
        response = await self.run_prompt(self.SUMMARIZE_RESULTS_PROMPT_NAME, timeout=20)
        if (not response):
            logger.error("No response from SummarizeResults prompt")
            return
        self.handler.summary = response["summary"]
        # Build a proper Message object, store it so runQuery() will return it,
        # and await sending so the send completes before runQuery returns.
        summary_content = {"@type": "Summary", "content": self.handler.summary}
        summary_msg = Message(
            sender_type=SenderType.ASSISTANT,
            message_type=MessageType.RESULT,
            content=[summary_content],
            conversation_id=self.handler.conversation_id
        )
        # Append to handler.messages so runQuery() includes it in its return value
        self.handler.messages.append(summary_msg)
        logger.info("Sending summary message to client", summary_msg.to_dict())
        # Await the send so it finishes before runQuery() continues/returns
        await self.handler.send_message(summary_msg.to_dict())
        # Use proper state update
        await self.handler.state.precheck_step_done("post_ranking")
