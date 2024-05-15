import grpc
import time
from concurrent import futures

import market_pb2
import market_pb2_grpc
import notificationService_pb2
import notificationService_pb2_grpc


item_database = {}
seller_database = {}
buyers_database = {}


class MarketServicer(market_pb2_grpc.MarketServiceServicer):
    
    def RegisterSeller(self, request, context):

        seller_info = request.seller_info
        
        if seller_info.uuid not in seller_database and seller_info.address not in [seller.address for seller in seller_database.values()]:
            print("\nSeller Join Request from:- ")
            print("Address: ", seller_info.address)
            print("UUID   : ", seller_info.uuid)

            seller_database[seller_info.uuid] = seller_info
            return market_pb2.RegisterSellerResponse(status=market_pb2.RegisterSellerResponse.SUCCESS)
        
        else:
            print("\nSeller Already Registered.")
            return market_pb2.RegisterSellerResponse(status=market_pb2.RegisterSellerResponse.FAILED)
    

    def SellItem(self, request, context):

        seller_info = request.seller_info

        if seller_info.uuid not in seller_database or seller_info.address != seller_database[seller_info.uuid].address:
            print("\nSeller not Registered.")
            return market_pb2.SellItemResponse(status=market_pb2.SellItemResponse.FAILED, item_id=-1)
        
        print("\nSeller Item Request from:- ")
        print("Address: ", seller_info.address)
        print("UUID   : ", seller_info.uuid)

        item = request.item
        item_id = len(item_database) + 1
        item.id = item_id
        item_database[item_id] = item

        return market_pb2.SellItemResponse(status=market_pb2.SellItemResponse.SUCCESS, item_id=item_id)
    

    def UpdateItem(self, request, context):

        seller_info = request.seller_info

        try:
            if seller_info.uuid not in seller_database or seller_info.address != item_database[request.item_id].sellerAddress:
                print("\nAn Unauthorized Seller is trying to Update an Item. Request Denied")
                return market_pb2.SellItemResponse(status=market_pb2.SellItemResponse.FAILED, item_id=-1)
            
        except KeyError:
            print("\nItem not found with the given ID.")
            return market_pb2.UpdateItemResponse(status=market_pb2.UpdateItemResponse.FAILED)

        if request.item_id in item_database:
            item = item_database[request.item_id]
            item.pricePerUnit = request.new_price
            item.quantity = request.new_quantity

            print("\nItem Updated Successfully:- ")
            print("Item ID: ", request.item_id)
            print("Request from Address: ", seller_info.address)

            for key, value in buyers_database.items():
                if request.item_id in value:
                    try:
                        channel = grpc.insecure_channel(key)
                        stub = notificationService_pb2_grpc.NotificationServiceStub(channel)
                        stub.NotifyClient(notificationService_pb2.NotifyClientRequest(id=request.item_id, productName=item.productName, category=item.category, quantity=item.quantity, description=item.description, pricePerUnit=item.pricePerUnit, rating=item.rating, sellerAddress=item.sellerAddress))

                    except Exception:
                        print("\nFailed to Notify Buyer.")
                        pass

            return market_pb2.UpdateItemResponse(status=market_pb2.UpdateItemResponse.SUCCESS)
        
        else:
            print("\nItem not found with the given ID.")
            return market_pb2.UpdateItemResponse(status=market_pb2.UpdateItemResponse.FAILED)
    

    def DeleteItem(self, request, context):

        seller_info = request.seller_info

        try:
            if seller_info.uuid not in seller_database or seller_info.address != item_database[request.item_id].sellerAddress:
                print("\nAn Unauthorized Seller is trying to Delete an Item. Request Denied")
                return market_pb2.SellItemResponse(status=market_pb2.SellItemResponse.FAILED, item_id=-1)
            
        except KeyError:
            print("\nItem not found with the given ID.")
            return market_pb2.DeleteItemResponse(status=market_pb2.DeleteItemResponse.FAILED)

        if request.item_id in item_database:
            del item_database[request.item_id]

            print("\nItem Deleted Successfully.")
            print("Item ID: ", request.item_id)
            print("Request from Address: ", seller_info.address)

            return market_pb2.DeleteItemResponse(status=market_pb2.DeleteItemResponse.SUCCESS)
        
        else:
            print("\nItem not found with the given ID.")
            return market_pb2.DeleteItemResponse(status=market_pb2.DeleteItemResponse.FAILED)
    

    def DisplaySellerItems(self, request, context):

        seller_info = request.seller_info

        if seller_info.uuid not in seller_database or seller_info.address != seller_database[seller_info.uuid].address:
            print("\nAn Unauthorized Seller is trying to Display Items. Request Denied")
            return market_pb2.SellItemResponse(status=market_pb2.SellItemResponse.FAILED, item_id=-1)
        
        print("\nDisplay Items Request from:- ")
        print("Address: ", seller_info.address)
        print("UUID   : ", seller_info.uuid)

        seller_items = [item for item in item_database.values() if item.sellerAddress == seller_info.address]
        return market_pb2.DisplaySellerItemsResponse(items=seller_items)
    

    def SearchItem(self, request, context):

        item_name = request.itemName

        if request.category == 0:
            category = "ELECTRONICS"
        
        elif request.category == 1:
            category = "FASHION"
        
        elif request.category == 2:
            category = "OTHERS"

        else:
            category = "ANY"

        print("\nSearch Request:- ")
        print("Item Name: ", item_name)
        print("Category : ", category)

        if not item_name and request.category == market_pb2.SearchItemRequest.ANY:
            items = item_database.values()

        elif not item_name:
            items = [item for item in item_database.values() if item.category == category]

        elif request.category == market_pb2.SearchItemRequest.ANY:
            items = [item for item in item_database.values() if item.productName.lower() == item_name.lower()]

        else:
            items = [item for item in item_database.values() if item.productName.lower() == item_name.lower() and item.category == category]

        return market_pb2.SearchItemResponse(items=items)
    

    def BuyItem(self, request, context):

        item_id = request.itemId
        quantity = request.quantity
        buyer_address = request.buyerAddress

        print("\nBuy Request:- ")
        print("Item ID: ", item_id)
        print("Quantity: ", quantity)
        print("Buyer Address: ", buyer_address)
        
        if item_id in item_database:
            item = item_database[item_id]

            if item.quantity >= quantity:
                item.quantity -= quantity

                try:
                    channel = grpc.insecure_channel(item.sellerAddress)
                    stub = notificationService_pb2_grpc.NotificationServiceStub(channel)
                    stub.NotifyClient(notificationService_pb2.NotifyClientRequest(id=item_id, productName=item.productName, category=item.category, quantity=item.quantity, description=item.description, pricePerUnit=item.pricePerUnit, rating=item.rating, sellerAddress=item.sellerAddress))
                
                except Exception:
                    print("\nFailed to Notify Seller.")
                    pass

                return market_pb2.BuyItemResponse(status=market_pb2.BuyItemResponse.SUCCESS)
            
            else:
                print("\nInsufficient Quantity.")
                return market_pb2.BuyItemResponse(status=market_pb2.BuyItemResponse.FAILED)
            
        else:
            print("\nItem not found with the given ID.")
            return market_pb2.BuyItemResponse(status=market_pb2.BuyItemResponse.FAILED)
    
    
    def AddToWishList(self, request, context):

        item_id = request.itemId
        buyer_address = request.buyerAddress

        print("\nWishlist Request:- ")
        print("Item ID: ", item_id)
        print("Buyer Address: ", buyer_address)
        
        if item_id in item_database:
            if buyer_address not in buyers_database:
                buyers_database[buyer_address] = set()

            if item_id in buyers_database[buyer_address]:
                print("\nItem already in Wishlist.")
                return market_pb2.AddToWishListResponse(status=market_pb2.AddToWishListResponse.FAILED)
            
            buyers_database[buyer_address].add(item_id)
            
            return market_pb2.AddToWishListResponse(status=market_pb2.AddToWishListResponse.SUCCESS)
        
        else:
            print("\nItem not found with the given ID.")
            return market_pb2.AddToWishListResponse(status=market_pb2.AddToWishListResponse.FAILED)
    

    def RateItem(self, request, context):

        item_id = request.itemId
        buyer_address = request.buyerAddress
        rating = request.rating
        
        if item_id in item_database:
            if buyer_address in buyers_database.get(item_id, {}):
                print("\nBuyer already Rated the Item.")
                return market_pb2.RateItemResponse(status=market_pb2.RateItemResponse.FAILED)
            
            print("\nRating Request:- ")
            print("Item ID: ", item_id)
            print("Buyer Address: ", buyer_address)
            print("Rating: ", str(rating) + " Stars")

            if item_id not in buyers_database:
                buyers_database[item_id] = {}

            buyers_database[item_id][buyer_address] = rating
            
            item = item_database[item_id]
            current_rating = item.rating
            num_buyers = len(buyers_database[item_id])
            item.rating = (current_rating * (num_buyers - 1) + rating) / num_buyers

            return market_pb2.RateItemResponse(status=market_pb2.RateItemResponse.SUCCESS)
        
        else:
            print("\nItem not found with the given ID.")
            return market_pb2.RateItemResponse(status=market_pb2.RateItemResponse.FAILED)


if __name__ == '__main__':
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    market_pb2_grpc.add_MarketServiceServicer_to_server(MarketServicer(), server)
    server.add_insecure_port('0.0.0.0:50051')
    server.start()

    print("\nMarket Server Started. Listening on Port: 50051\n")

    try:
        while True:
            time.sleep(86400)
            
    except KeyboardInterrupt:
        print("\n\n\nGoodbye!\n\n")
        server.stop(0)