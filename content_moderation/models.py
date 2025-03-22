from django.db import models
import uuid

class Priority(models.IntegerChoices):
    LOW = 1, 'Low'
    MEDIUM = 2, 'Medium'
    HIGH = 3, 'High'
class Status(models.TextChoices):
    PENDING = 'PEN', 'Pending'
    APPROVED = 'APP', 'Approved'
    REJECTED = 'REJ', 'Rejected'

class DocumentType(models.TextChoices):
    PDF = 'PDF', 'PDF Document'
    JPG = 'JPG', 'JPEG Image'
    PNG = 'PNG', 'PNG Image'

class ContentTypeEnum(models.TextChoices):
    WEBSITE = 'website', 'Website'
    MEDIA = 'media', 'Media'
    DOCUMENT = 'document', 'Document'
    SOCIAL_MEDIA_POST = 'social media post', 'Social Media Post'
    OTHER = 'other', 'Other'

# Create your models here.
class Request(models.Model):
    request_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    content_type = models.CharField(max_length=50, choices=ContentTypeEnum.choices)
    priority = models.IntegerField(choices=Priority.choices, default=Priority.LOW)
    content_url = models.URLField(max_length=500)
    description = models.TextField(max_length=1000)
    email = models.EmailField(max_length=255)
    approval_status = models.CharField(max_length=3, choices=Status.choices)
    created_at = models.DateTimeField(auto_now_add=True)

class Document(models.Model):
    document_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    request = models.ForeignKey(Request, on_delete=models.PROTECT, related_name='documents')
    document_title = models.CharField(max_length=255)
    document_url = models.URLField(max_length=500)
    document_type = models.CharField(max_length=3, choices=DocumentType.choices)
    created_at = models.DateTimeField(auto_now_add=True)