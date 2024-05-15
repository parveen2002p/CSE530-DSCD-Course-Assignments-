import sys
import grpc
import uuid
from concurrent import futures

import market_pb2
import market_pb2_grpc
import notificationService_pb2
import notificationService_pb2_grpc


seller_address = ""
seller_uuid = str(uuid.uuid1())


def register_seller(stub):

    response = stub.RegisterSeller(market_pb2.RegisterSellerRequest(seller_info=market_pb2.SellerInfo(address=seller_address, uuid=seller_uuid)))

    if response.status == market_pb2.RegisterSellerResponse.SUCCESS:
        print("\nSUCCESS: Seller Registered Successfully.")

    else:
        print("\nFAILED: Seller Registration Failed.")


def sell_item(stub):

    product_name = input("\nEnter Product Name: ")
    category = input("Enter Category (ELECTRONICS, FASHION, or OTHERS): ")
    quantity = int(input("Enter Quantity: "))
    description = input("Enter Description: ")
    price_per_unit = float(input("Enter Price per Unit: "))

    response = stub.SellItem(market_pb2.SellItemRequest(seller_info=market_pb2.SellerInfo(address=seller_address, uuid=seller_uuid),
        item=market_pb2.Item(productName=product_name, category=category, quantity=quantity, description=description, sellerAddress=seller_address, pricePerUnit=price_per_unit)))

    if response.status == market_pb2.SellItemResponse.SUCCESS:
        print("\nSUCCESS: New Item Successfully Added to the Market.")
        print("Unique Item ID:", response.item_id)

    else:
        print("\nFAILED: Failed to add New item to the Market.")


def update_item(stub):
    
    item_id = int(input("Enter Item ID to Update: "))
    new_price = float(input("Enter New Price: "))
    new_quantity = int(input("Enter New Quantity: "))

    response = stub.UpdateItem(market_pb2.UpdateItemRequest(seller_info=market_pb2.SellerInfo(address=seller_address, uuid=seller_uuid), item_id=item_id, new_price=new_price, new_quantity=new_quantity))

    if response.status == market_pb2.UpdateItemResponse.SUCCESS:
        print("\nSUCCESS: Item Updated Successfully.")

    else:
        print("\nFAILED: Failed to Update Item.")


def delete_item(stub):

    item_id = int(input("Enter Item ID to Delete: "))

    response = stub.DeleteItem(market_pb2.DeleteItemRequest(seller_info=market_pb2.SellerInfo(address=seller_address, uuid=seller_uuid), item_id=item_id))
    
    if response.status == market_pb2.DeleteItemResponse.SUCCESS:
        print("\nSUCCESS: Item Deleted Successfully.")

    else:
        print("\nFAILED: Failed to Delete Item.")


def display_seller_items(stub):

    response = stub.DisplaySellerItems(market_pb2.DisplaySellerItemsRequest(seller_info=market_pb2.SellerInfo(address=seller_address, uuid=seller_uuid)))
    
    if response.status == market_pb2.DisplaySellerItemsResponse.SUCCESS:
        if len(response.items) == 0:
            print("\nNo Items Found for the Seller.")
            return
        
        print("\nSeller's Items:- ")
        print("-" * 139)
        print("|{:^5} | {:^15} | {:^11} | {:^8} | {:^30} | {:^15} | {:^10} | {:^22}|".format("ID", "Product Name", "Category", "Quantity", "Description", "Price per Unit", "Rating (5)", "Seller Address"))
        print("-" * 139)

        for item in response.items:
            print("|{:^5} | {:^15} | {:^11} | {:^8} | {:^30} | {:^15} | {:^10} | {:^22}|".format(item.id, item.productName, item.category, item.quantity, item.description, item.pricePerUnit, item.rating, item.sellerAddress))
            print("-" * 139)
    
    else:
        print("\nFAILED: Failed to Display Seller's Items.")


class NotificationService(notificationService_pb2_grpc.NotificationServiceServicer):

    def NotifyClient(self, request, context):
        
        print("\n\nNotification Received:- ")
        print("\nThe following Item has been Sold:- ")
        print("-" * 149)
        print("|{:^5} | {:^15} | {:^11} | {:^15} | {:^30} | {:^15} | {:^10} | {:^22}|".format("ID", "Product Name", "Category", "Quantity Remaining", "Description", "Price per Unit", "Rating (5)", "Seller Address"))
        print("-" * 149)
        print("|{:^5} | {:^15} | {:^11} | {:^18} | {:^30} | {:^15} | {:^10} | {:^22}|".format(request.id, request.productName, request.category, request.quantity, request.description, request.pricePerUnit, request.rating, request.sellerAddress))
        print("-" * 149)

        print("\n1. Register Seller\n2. Sell Item\n3. Update Item\n4. Delete Item\n5. Display Seller Items\n6. Exit\n\nEnter your Choice:- ")

        return notificationService_pb2.NotifyClientResponse(status=notificationService_pb2.NotifyClientResponse.SUCCESS)


if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("\nUsage: python3 seller.py <MarketServerIP:Port> <SellerServerIP:Port>\n")
        sys.exit(1)

    seller_address = sys.argv[2]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notificationService_pb2_grpc.add_NotificationServiceServicer_to_server(NotificationService(), server)
    server.add_insecure_port("0.0.0.0:" + sys.argv[2].split(":")[1])
    server.start()

    print("\nSeller's Notification Server Started. Listening on " + sys.argv[2] + "\n")

    channel = grpc.insecure_channel(sys.argv[1])
    stub = market_pb2_grpc.MarketServiceStub(channel)
    
    try:
        while True:
            print("\n1. Register Seller\n2. Sell Item\n3. Update Item\n4. Delete Item\n5. Display Seller Items\n6. Exit")
            choice = input("\nEnter your Choice: ")

            if choice == '1':
                register_seller(stub)

            elif choice == '2':
                sell_item(stub)

            elif choice == '3':
                update_item(stub)

            elif choice == '4':
                delete_item(stub)

            elif choice == '5':
                display_seller_items(stub)

            elif choice == '6':
                print("\n\nGoodbye!\n\n")
                break

            else:
                print("\nInvalid choice. Please try again.")

    except KeyboardInterrupt:
        print("\n\n\nGoodbye!\n\n")
        server.stop(0)