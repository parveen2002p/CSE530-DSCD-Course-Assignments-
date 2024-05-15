import sys
import grpc
from concurrent import futures

import market_pb2
import market_pb2_grpc
import notificationService_pb2
import notificationService_pb2_grpc


buyer_address = ""


def search_item(stub):

    item_name = input("\nEnter Item Name (leave blank to display all items): ")
    category  = input("Enter Category (ELECTRONICS, FASHION, OTHERS, ANY): ")

    category_map = {"ANY": market_pb2.SearchItemRequest.ANY, "OTHERS": market_pb2.SearchItemRequest.OTHERS, "FASHION": market_pb2.SearchItemRequest.FASHION, "ELECTRONICS": market_pb2.SearchItemRequest.ELECTRONICS}

    if category.upper() in category_map:
        response = stub.SearchItem(market_pb2.SearchItemRequest(itemName=item_name, category=category_map[category.upper()]))

        if len(response.items) != 0:
            print("\nSearch Results:- ")
            print("-" * 139)
            print("|{:^5} | {:^15} | {:^11} | {:^8} | {:^30} | {:^15} | {:^10} | {:^22}|".format("ID", "Product Name", "Category", "Quantity", "Description", "Price per Unit", "Rating (5)", "Seller Address"))
            print("-" * 139)

            for item in response.items:
                print("|{:^5} | {:^15} | {:^11} | {:^8} | {:^30} | {:^15} | {:^10} | {:^22}|".format(item.id, item.productName, item.category, item.quantity, item.description, item.pricePerUnit, item.rating, item.sellerAddress))
                print("-" * 139)

        else:
            print("\nNo items found with the given Criteria.")

    else:
        print("\nInvalid Category.")


def buy_item(stub):

    item_id = int(input("Enter Item ID to Buy: "))
    quantity = int(input("Enter Quantity to Buy: "))

    response = stub.BuyItem(market_pb2.BuyItemRequest(itemId=item_id, quantity=quantity, buyerAddress=buyer_address))

    if response.status == market_pb2.BuyItemResponse.SUCCESS:
        print("\nSUCCESS: Item bought Successfully.")

    else:
        print("\nFAILED: Failed to buy Item.")


def add_to_wishlist(stub):

    item_id = int(input("\nEnter Item ID to Add to Wishlist: "))

    response = stub.AddToWishList(market_pb2.AddToWishListRequest(itemId=item_id, buyerAddress=buyer_address))

    if response.status == market_pb2.AddToWishListResponse.SUCCESS:
        print("\nSUCCESS: Item added to wishlist Successfully.")

    else:
        print("\nFAILED: Failed to Add Item to Wishlist.")


def rate_item(stub):

    item_id = int(input("\nEnter Item ID to Rate: "))
    rating = int(input("Enter Rating (1 to 5 Stars): "))

    if rating < 1 or rating > 5:
        print("\nFAILED: Invalid rating. Rating must be between 1 and 5 stars.")
        return
    
    response = stub.RateItem(market_pb2.RateItemRequest(itemId=item_id, buyerAddress=buyer_address, rating=rating))

    if response.status == market_pb2.RateItemResponse.SUCCESS:
        print("\nSUCCESS: Item Rated Successfully.")

    else:
        print("\nFAILED: Failed to Rate Item.")


class NotificationService(notificationService_pb2_grpc.NotificationServiceServicer):

    def NotifyClient(self, request, context):
        
        print("\n\nNotification Received:- ")
        print("\nThe following Item From Your Wishlist has been Updated:- ")
        print("-" * 139)
        print("|{:^5} | {:^15} | {:^11} | {:^8} | {:^30} | {:^15} | {:^10} | {:^22}|".format("ID", "Product Name", "Category", "Quantity", "Description", "Price per Unit", "Rating (5)", "Seller Address"))
        print("-" * 139)
        print("|{:^5} | {:^15} | {:^11} | {:^8} | {:^30} | {:^15} | {:^10} | {:^22}|".format(request.id, request.productName, request.category, request.quantity, request.description, request.pricePerUnit, request.rating, request.sellerAddress))
        print("-" * 139)

        print("\n1. Search Item\n2. Buy Item\n3. Add to Wishlist\n4. Rate Item\n5. Exit\n\nEnter your Choice:- ")

        return notificationService_pb2.NotifyClientResponse(status=notificationService_pb2.NotifyClientResponse.SUCCESS)


if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("\nUsage: python buyer.py <MarketServerIP:Port> <BuyerServerIP:Port>")
        sys.exit(1)
        
    buyer_address = sys.argv[2]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notificationService_pb2_grpc.add_NotificationServiceServicer_to_server(NotificationService(), server)
    server.add_insecure_port("0.0.0.0:" + sys.argv[2].split(":")[1])
    server.start()

    print("\nBuyer's Notification Server Started. Listening on " + sys.argv[2] + "\n")

    channel = grpc.insecure_channel(sys.argv[1])
    stub = market_pb2_grpc.MarketServiceStub(channel)
    
    try:
        while True:
            print("\n1. Search Item\n2. Buy Item\n3. Add to Wishlist\n4. Rate Item\n5. Exit")
            choice = input("\nEnter your Choice: ")

            if choice == '1':
                search_item(stub)

            elif choice == '2':
                buy_item(stub)

            elif choice == '3':
                add_to_wishlist(stub)

            elif choice == '4':
                rate_item(stub)

            elif choice == '5':
                print("\n\nGoodbye!\n\n")
                break

            else:
                print("Invalid choice. Please try again.")

    except KeyboardInterrupt:
        print("\n\n\nGoodbye!\n\n")
        server.stop(0)